package clickhouse

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type Client struct {
	db *sql.DB
}

type Answer struct {
	ChatID         string    `json:"chat_id"`
	AnswerID       string    `json:"answer_id"`
	SentAt         time.Time `json:"sent_at"`
	ConversationID string    `json:"conversation_id,omitempty"`
	AssistantID    string    `json:"assistant_id,omitempty"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	Raw            string    `json:"raw,omitempty"`
}

type DailyCount struct {
	Date  string `json:"date"`
	Count int64  `json:"count"`
}

func Open(dsn string) (*Client, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Client{db: db}, nil
}

func (c *Client) InsertAnswers(ctx context.Context, answers []Answer) error {
	if len(answers) == 0 {
		return nil
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO analytics.answers_raw
(chat_id, answer_id, sent_at, created_at)
VALUES (?, ?, ?, now())
`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, a := range answers {
		if _, err := stmt.ExecContext(ctx, a.ChatID, a.AnswerID, a.CreatedAt); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (c *Client) DailyCounts(ctx context.Context, chatID string, since, until time.Time) ([]DailyCount, error) {
	query := `
SELECT
  formatDateTime(toStartOfDay(created_at), '%Y-%m-%d') AS date,
  count() AS count
FROM analytics.answers_raw
WHERE chat_id = ?
  AND created_at >= toDateTime64(?, 3, 'UTC')
  AND created_at < toDateTime64(?, 3, 'UTC')
GROUP BY date
ORDER BY date ASC
`
	rows, err := c.db.QueryContext(ctx, query, chatID, since, until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]DailyCount, 0)
	for rows.Next() {
		var item DailyCount
		if err := rows.Scan(&item.Date, &item.Count); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (c *Client) Close() error {
	return c.db.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}
