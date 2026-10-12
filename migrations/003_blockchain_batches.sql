BEGIN;

CREATE TABLE IF NOT EXISTS blockchain_batches (
    id UUID PRIMARY KEY,
    chain_id TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    merkle_root TEXT NOT NULL UNIQUE,
    leaf_count INTEGER NOT NULL CHECK (leaf_count > 0),
    status TEXT NOT NULL CHECK (status IN ('SUBMITTING', 'SUBMITTED', 'CONFIRMED', 'FAILED')),
    tx_hash TEXT,
    block_number BIGINT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_at TIMESTAMPTZ
);

ALTER TABLE blockchain_proofs
    ADD COLUMN IF NOT EXISTS batch_id UUID REFERENCES blockchain_batches(id),
    ADD COLUMN IF NOT EXISTS leaf_index INTEGER,
    ADD COLUMN IF NOT EXISTS leaf_hash TEXT,
    ADD COLUMN IF NOT EXISTS merkle_path JSONB;

CREATE INDEX IF NOT EXISTS idx_blockchain_proofs_batch ON blockchain_proofs(batch_id);
CREATE INDEX IF NOT EXISTS idx_blockchain_proofs_claim_hash ON blockchain_proofs(claim_hash);

COMMIT;
