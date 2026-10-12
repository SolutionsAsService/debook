package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion uint16          `json:"schema_version"`
	AggregateID   string          `json:"aggregate_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	CorrelationID *string         `json:"correlation_id"`
	CausationID   *string         `json:"causation_id"`
	Producer      string          `json:"producer"`
	Payload       json.RawMessage `json:"payload"`
}
type bookingPayload struct {
	BookingID     string     `json:"booking_id"`
	SlotID        string     `json:"slot_id"`
	Status        string     `json:"status"`
	TenantID      string     `json:"tenant_id"`
	CustomerRef   string     `json:"customer_ref"`
	WalletAddress string     `json:"wallet_address"`
	RegionID      string     `json:"region_id"`
	ResourceID    string     `json:"resource_id"`
	ExpiresAt     *time.Time `json:"expires_at"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: strings.Split(getenv("KAFKA_BROKERS", "kafka:19092"), ","), Topic: getenv("KAFKA_TOPIC", "booking.events"), GroupID: getenv("KAFKA_GROUP", "clickhouse-analytics"), MinBytes: 1, MaxBytes: 10e6})
	defer r.Close()
	client := &http.Client{Timeout: 10 * time.Second}
	endpoint := getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("kafka", "error", err)
			continue
		}
		if err := ingest(ctx, client, endpoint, msg.Value); err != nil {
			log.Error("clickhouse", "error", err)
			continue
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			log.Error("kafka commit", "error", err)
			continue
		}
		log.Info("ingested event", "offset", msg.Offset)
	}
}
func ingest(ctx context.Context, client *http.Client, endpoint string, body []byte) error {
	var e envelope
	if err := json.Unmarshal(body, &e); err != nil {
		return err
	}
	if _, err := uuid.Parse(e.EventID); err != nil {
		return fmt.Errorf("event_id: %w", err)
	}
	if _, err := uuid.Parse(e.AggregateID); err != nil {
		return fmt.Errorf("aggregate_id: %w", err)
	}
	var raw bytes.Buffer
	enc := json.NewEncoder(&raw)
	enc.SetEscapeHTML(false)
	row := map[string]any{"event_id": e.EventID, "event_type": e.EventType, "schema_version": e.SchemaVersion, "aggregate_id": e.AggregateID, "occurred_at": clickhouseTime(e.OccurredAt), "correlation_id": e.CorrelationID, "causation_id": e.CausationID, "producer": e.Producer, "payload": string(e.Payload)}
	_ = enc.Encode(row)
	if err := post(ctx, client, endpoint, "INSERT INTO debook.event_log FORMAT JSONEachRow", raw.Bytes()); err != nil {
		return err
	}
	var p bookingPayload
	if json.Unmarshal(e.Payload, &p) == nil && p.BookingID != "" {
		raw.Reset()
		var expires any
		if p.ExpiresAt != nil {
			expires = clickhouseTime(*p.ExpiresAt)
		}
		_ = enc.Encode(map[string]any{"event_id": e.EventID, "booking_id": p.BookingID, "slot_id": p.SlotID, "tenant_id": p.TenantID, "customer_ref": p.CustomerRef, "wallet_address": p.WalletAddress, "event_type": e.EventType, "booking_status": p.Status, "region_id": p.RegionID, "resource_id": p.ResourceID, "occurred_at": clickhouseTime(e.OccurredAt), "expires_at": expires})
		if err := post(ctx, client, endpoint, "INSERT INTO debook.booking_facts FORMAT JSONEachRow", raw.Bytes()); err != nil {
			return err
		}
	}
	return nil
}
func clickhouseTime(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000") }
func post(ctx context.Context, c *http.Client, endpoint, q string, b []byte) error {
	v := url.Values{"user": {getenv("CLICKHOUSE_USER", "debook")}, "password": {getenv("CLICKHOUSE_PASSWORD", "debook_local")}, "query": {q}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/?"+v.Encode(), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		message, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	return nil
}
func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
