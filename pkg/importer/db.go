package importer

import (
	"database/sql"
	"log"

	"github.com/lib/pq"
)

func movieListExists(db *sql.DB, md5Hash string) (bool, error) {
	return false, nil
}

func createAndPrepareSchema(db *sql.DB, schema string) error {
	return nil
}

func bulkCopyChannelEntries(db *sql.DB, schema string, entries map[string]int64) error {
	return bulkCopyMappedEntries(db, schema, "channels", entries)
}

func bulkCopyTopicEntries(db *sql.DB, schema string, entries map[string]int64) error {
	return bulkCopyMappedEntries(db, schema, "topics", entries)
}

func bulkCopyMappedEntries(db *sql.DB, schema, tableName string, entries map[string]int64) error {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyInSchema(schema, tableName, "id", "name"))
	if err != nil {
		return err
	}

	// Note that the entry key is the name and the value is the ID
	for k := range entries {
		_, err = stmt.Exec(entries[k], k)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}

// bulkCopyMovieEntries adds the movie entries to the given database and schema
func bulkCopyMovieEntries(db *sql.DB, schema string, entries []movieEntry) error {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyInSchema(schema,
		"movies", "channel", "channel_id", "topic", "topic_id", "title",
		"published_at", "duration", "size", "descr", "url", "website_url",
		"sub_title_url", "small_format_url", "hd_format_url", "unix_date",
		"history_url", "geo", "is_new"))
	if err != nil {
		return err
	}

	for _, entry := range entries {
		_, err = stmt.Exec(entry.channel, entry.channelID, entry.topic,
			entry.topicID, entry.title, entry.publishedAt, entry.duration,
			entry.size, entry.descr, entry.url, entry.websiteURL,
			entry.subTitleURL, entry.smallFormatURL, entry.hdFormatURL,
			entry.unixDate, entry.historyURL, entry.geo, entry.isNew)
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}
