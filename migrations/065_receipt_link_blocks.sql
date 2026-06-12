CREATE TABLE IF NOT EXISTS receipt_link_blocks (
    paperless_id INTEGER NOT NULL,
    txn_hash TEXT NOT NULL,
    reason TEXT,
    blocked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (paperless_id, txn_hash)
);

CREATE INDEX IF NOT EXISTS idx_receipt_link_blocks_txn
    ON receipt_link_blocks (txn_hash);
