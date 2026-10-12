package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type envelope struct {
	EventID   string `json:"event_id"`
	BookingID string `json:"aggregate_id"`
	Type      string `json:"event_type"`
	Payload   struct {
		WalletAddress string `json:"wallet_address"`
	} `json:"payload"`
}

type proofRow struct {
	bookingID uuid.UUID
	claimHash [32]byte
	wallet    common.Address
	leaf      [32]byte
	index     int
}

const contractABI = `[
 {"inputs":[{"internalType":"bytes32","name":"batchId","type":"bytes32"},{"internalType":"bytes32","name":"merkleRoot","type":"bytes32"},{"internalType":"uint256","name":"leafCount","type":"uint256"}],"name":"commitBatch","outputs":[],"stateMutability":"nonpayable","type":"function"}
]`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	l := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	db, err := pgxpool.New(ctx, getenv("DATABASE_URL", "postgres://debook:debook_local@postgres:5432/debook"))
	if err != nil {
		l.Error("db", "error", err)
		return
	}
	defer db.Close()
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{getenv("KAFKA_BROKERS", "kafka:19092")}, Topic: getenv("KAFKA_TOPIC", "booking.events"), GroupID: "booking-workflow", MinBytes: 1, MaxBytes: 10e6})
	defer r.Close()
	client, err := ethclient.Dial(getenv("AUTHEO_RPC_URL", ""))
	if err != nil {
		l.Error("autheo rpc", "error", err)
		return
	}
	defer client.Close()
	key, err := crypto.HexToECDSA(strings.TrimPrefix(getenv("AUTHEO_PRIVATE_KEY", ""), "0x"))
	if err != nil {
		l.Error("autheo private key", "error", err)
		return
	}
	chainID, ok := new(big.Int).SetString(getenv("AUTHEO_CHAIN_ID", "21270"), 10)
	if !ok {
		l.Error("invalid chain id")
		return
	}
	parsed, err := abi.JSON(strings.NewReader(contractABI))
	if err != nil {
		l.Error("contract ABI", "error", err)
		return
	}
	contract := bind.NewBoundContract(common.HexToAddress(getenv("AUTHEO_CONTRACT_ADDRESS", "")), parsed, client, client, client)
	batchSize := envInt("BLOCKCHAIN_BATCH_SIZE", 1000)
	flushInterval := envDuration("BLOCKCHAIN_BATCH_INTERVAL", 5*time.Second)
	if batchSize < 1 {
		batchSize = 1000
	}
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	flush := func() {
		count, err := pendingProofCount(ctx, db)
		if err != nil {
			l.Error("count pending proofs", "error", err)
			return
		}
		if count == 0 {
			return
		}
		if err := createBatch(ctx, db, client, contract, key, chainID, batchSize); err != nil {
			l.Error("blockchain batch", "count", count, "error", err)
			return
		}
		l.Info("blockchain batch committed", "count", count)
	}
	for {
		readCtx, cancel := context.WithTimeout(ctx, flushInterval)
		m, err := r.FetchMessage(readCtx)
		cancel()
		if err == nil {
			var e envelope
			valid := json.Unmarshal(m.Value, &e) == nil
			if valid && e.Type == "BookingReserved" {
				if err := prepareProof(ctx, db, e); err != nil {
					l.Error("prepare blockchain proof", "booking_id", e.BookingID, "error", err)
					continue
				}
			}
			if !valid {
				l.Error("invalid kafka event", "offset", m.Offset)
			}
			if err := r.CommitMessages(ctx, m); err != nil {
				l.Error("commit kafka message", "error", err)
			}
		} else if ctx.Err() != nil {
			flush()
			return
		} else if !errors.Is(err, context.DeadlineExceeded) {
			l.Error("read kafka", "error", err)
		}
		count, countErr := pendingProofCount(ctx, db)
		if countErr == nil && count >= batchSize {
			flush()
		}
		select {
		case <-ticker.C:
			flush()
		default:
		}
	}
}

func prepareProof(ctx context.Context, db *pgxpool.Pool, e envelope) error {
	bookingID, err := uuid.Parse(e.BookingID)
	if err != nil {
		return err
	}
	var wallet string
	if err := db.QueryRow(ctx, `SELECT COALESCE(wallet_address,'') FROM bookings WHERE id=$1`, bookingID).Scan(&wallet); err != nil {
		return err
	}
	if !common.IsHexAddress(wallet) {
		_, _ = db.Exec(ctx, `UPDATE bookings SET status='FAILED',updated_at=now() WHERE id=$1 AND status='PENDING_CONFIRMATION'`, bookingID)
		_, _ = db.Exec(ctx, `INSERT INTO outbox_events(id,aggregate_id,type,payload) SELECT gen_random_uuid(),id,'BookingFailed',jsonb_build_object('booking_id',id::text,'slot_id',slot_id::text,'status','FAILED','tenant_id',tenant_id,'customer_ref',customer_ref,'wallet_address',COALESCE(wallet_address,'')) FROM bookings WHERE id=$1`, bookingID)
		_, _ = db.Exec(ctx, `INSERT INTO booking_steps(booking_id,step,status,attempt,last_error) VALUES($1,'blockchain','FAILED',1,$2) ON CONFLICT (booking_id,step) DO UPDATE SET status='FAILED',last_error=$2,updated_at=now()`, bookingID, "invalid wallet_address")
		return nil
	}
	claim := sha256.Sum256([]byte(bookingID.String()))
	claimHex := "0x" + hex.EncodeToString(claim[:])
	_, err = db.Exec(ctx, `INSERT INTO blockchain_proofs(booking_id,chain_id,contract_address,claim_hash,status) VALUES($1,$2,$3,$4,'PENDING') ON CONFLICT (booking_id) DO NOTHING`, bookingID, getenv("AUTHEO_CHAIN_ID", "21270"), getenv("AUTHEO_CONTRACT_ADDRESS", ""), claimHex)
	return err
}

func pendingProofCount(ctx context.Context, db *pgxpool.Pool) (int, error) {
	var count int
	err := db.QueryRow(ctx, `SELECT count(*) FROM blockchain_proofs WHERE status='PENDING' AND tx_hash IS NULL AND batch_id IS NULL`).Scan(&count)
	return count, err
}

func createBatch(ctx context.Context, db *pgxpool.Pool, client *ethclient.Client, contract *bind.BoundContract, key *ecdsa.PrivateKey, chainID *big.Int, batchSize int) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT p.booking_id,p.claim_hash,b.wallet_address FROM blockchain_proofs p JOIN bookings b ON b.id=p.booking_id WHERE p.status='PENDING' AND p.tx_hash IS NULL AND p.batch_id IS NULL ORDER BY p.created_at,p.booking_id FOR UPDATE OF p SKIP LOCKED LIMIT $1`, batchSize)
	if err != nil {
		return err
	}
	proofs := make([]proofRow, 0, batchSize)
	for rows.Next() {
		var p proofRow
		var claim string
		var wallet string
		if err := rows.Scan(&p.bookingID, &claim, &wallet); err != nil {
			rows.Close()
			return err
		}
		decoded, err := hex.DecodeString(strings.TrimPrefix(claim, "0x"))
		if err != nil || len(decoded) != 32 {
			rows.Close()
			return fmt.Errorf("invalid claim hash for %s", p.bookingID)
		}
		copy(p.claimHash[:], decoded)
		p.wallet = common.HexToAddress(wallet)
		proofs = append(proofs, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(proofs) == 0 {
		return nil
	}
	for i := range proofs {
		proofs[i].index = i
		proofs[i].leaf = makeLeaf(proofs[i].claimHash, proofs[i].wallet)
	}
	levels := merkleLevels(proofs)
	root := levels[len(levels)-1][0]
	batchID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO blockchain_batches(id,chain_id,contract_address,merkle_root,leaf_count,status) VALUES($1,$2,$3,$4,$5,'SUBMITTING')`, batchID, getenv("AUTHEO_CHAIN_ID", "21270"), getenv("AUTHEO_CONTRACT_ADDRESS", ""), common.BytesToHash(root[:]).Hex(), len(proofs)); err != nil {
		return err
	}
	for _, p := range proofs {
		path := merkleProof(levels, p.index)
		pathJSON, _ := json.Marshal(path)
		if _, err := tx.Exec(ctx, `UPDATE blockchain_proofs SET batch_id=$2,leaf_index=$3,leaf_hash=$4,merkle_path=$5,status='SUBMITTED' WHERE booking_id=$1`, p.bookingID, batchID, p.index, common.BytesToHash(p.leaf[:]).Hex(), pathJSON); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, `UPDATE blockchain_batches SET status='FAILED',last_error='submission interrupted before transaction hash was saved' WHERE status='SUBMITTING' AND created_at < now() - interval '2 minutes'`); err != nil {
		return err
	}

	batchHash := crypto.Keccak256Hash(batchID[:])
	auth, err := bind.NewKeyedTransactorWithChainID(key, chainID)
	if err != nil {
		return err
	}
	chainTx, err := contract.Transact(auth, "commitBatch", [32]byte(batchHash), root, big.NewInt(int64(len(proofs))))
	if err != nil {
		_, _ = db.Exec(ctx, `UPDATE blockchain_batches SET status='FAILED',last_error=$2 WHERE id=$1`, batchID, err.Error())
		return err
	}
	if _, err := db.Exec(ctx, `UPDATE blockchain_batches SET status='SUBMITTED',tx_hash=$2 WHERE id=$1`, batchID, chainTx.Hash().Hex()); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, `UPDATE blockchain_proofs SET tx_hash=$2 WHERE batch_id=$1`, batchID, chainTx.Hash().Hex()); err != nil {
		return err
	}
	mined, err := bind.WaitMined(ctx, client, chainTx)
	if err != nil {
		return err
	}
	if mined.Status != 1 {
		_, _ = db.Exec(ctx, `UPDATE blockchain_batches SET status='FAILED',last_error='transaction reverted' WHERE id=$1`, batchID)
		return errors.New("blockchain transaction reverted")
	}
	confirmations := envInt("AUTHEO_CONFIRMATIONS", 2)
	if confirmations < 1 {
		confirmations = 1
	}
	for {
		height, err := client.BlockNumber(ctx)
		if err != nil {
			return err
		}
		if height >= mined.BlockNumber.Uint64()+uint64(confirmations)-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if _, err := db.Exec(ctx, `UPDATE blockchain_batches SET status='CONFIRMED',block_number=$2,confirmed_at=now() WHERE id=$1`, batchID, mined.BlockNumber.Uint64()); err != nil {
		return err
	}
	for _, p := range proofs {
		if _, err := db.Exec(ctx, `UPDATE blockchain_proofs SET status='CONFIRMED',tx_hash=$2,confirmations=$3,confirmed_at=now() WHERE booking_id=$1`, p.bookingID, chainTx.Hash().Hex(), confirmations); err != nil {
			return err
		}
		if _, err := db.Exec(ctx, `INSERT INTO booking_steps(booking_id,step,status,attempt) VALUES($1,'blockchain','SUCCEEDED',1) ON CONFLICT (booking_id,step) DO UPDATE SET status='SUCCEEDED',updated_at=now()`, p.bookingID); err != nil {
			return err
		}
		if _, err := db.Exec(ctx, `UPDATE bookings SET status='CONFIRMED',updated_at=now() WHERE id=$1 AND status='PENDING_CONFIRMATION'`, p.bookingID); err != nil {
			return err
		}
		_, _ = db.Exec(ctx, `INSERT INTO outbox_events(id,aggregate_id,type,payload) SELECT gen_random_uuid(),id,'BookingConfirmed',jsonb_build_object('booking_id',id::text,'slot_id',slot_id::text,'status','CONFIRMED','tenant_id',tenant_id,'customer_ref',customer_ref,'wallet_address',COALESCE(wallet_address,'')) FROM bookings WHERE id=$1`, p.bookingID)
	}
	return nil
}

func makeLeaf(claim [32]byte, wallet common.Address) [32]byte {
	data := append(append([]byte{}, claim[:]...), wallet.Bytes()...)
	return [32]byte(crypto.Keccak256Hash(data))
}
func hashPair(a, b [32]byte) [32]byte {
	if string(a[:]) > string(b[:]) {
		a, b = b, a
	}
	return [32]byte(crypto.Keccak256Hash(append(append([]byte{}, a[:]...), b[:]...)))
}
func merkleLevels(proofs []proofRow) [][][32]byte {
	level := make([][32]byte, len(proofs))
	for i, p := range proofs {
		level[i] = p.leaf
	}
	levels := [][][32]byte{level}
	for len(level) > 1 {
		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i])
			} else {
				next = append(next, hashPair(level[i], level[i+1]))
			}
		}
		level = next
		levels = append(levels, level)
	}
	return levels
}
func merkleProof(levels [][][32]byte, index int) []string {
	path := make([]string, 0, len(levels)-1)
	for level := 0; level < len(levels)-1; level++ {
		sibling := index ^ 1
		if sibling >= len(levels[level]) {
			sibling = index
		}
		path = append(path, common.BytesToHash(levels[level][sibling][:]).Hex())
		index /= 2
	}
	return path
}
func envInt(k string, fallback int) int {
	var n int
	if _, err := fmt.Sscanf(os.Getenv(k), "%d", &n); err != nil {
		return fallback
	}
	return n
}
func envDuration(k string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(os.Getenv(k))
	if err != nil {
		return fallback
	}
	return d
}
func getenv(k, f string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return f
}
