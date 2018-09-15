package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	proto "github.com/golang/protobuf/proto"
	pb "github.com/tschokko/mdthk-api/pkg/moviecat"

	_ "github.com/lib/pq"
)

type movieEntity struct {
	id             int64
	channelID      int64
	topicID        int64
	title          string
	publishedAt    time.Time
	duration       string
	size           int64
	descr          string
	url            string
	websiteURL     string
	subTitleURL    string
	smallFormatURL string
	hdFormatURL    string
	unixDate       int64
	historyURL     string
	geo            string
	isNew          bool
}

func main() {
	connStr := "postgres://u4mdthk:pw4mdthk@localhost/mdthk?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	catalog := &pb.MovieCatalog{
		Version:     1,
		PublishedAt: time.Now().Unix(),
		Md5Hash:     []byte("abe56e1b444ef4f637971dfdf5c14ce1"),
	}

	channelEntites, _ := findAllChannels(db, "abe56e1b444ef4f637971dfdf5c14ce1")
	for id, name := range channelEntites {
		channelEntry := &pb.ChannelEntry{
			Version: 1,
			Id:      id,
			Name:    name,
		}
		catalog.Channels = append(catalog.Channels, channelEntry)
	}

	topicEntities, _ := findAllTopics(db, "abe56e1b444ef4f637971dfdf5c14ce1")
	for id, name := range topicEntities {
		topicEntry := &pb.TopicEntry{
			Version: 1,
			Id:      id,
			Name:    name,
		}
		catalog.Topics = append(catalog.Topics, topicEntry)
	}

	movieEntities, _ := findAllMovieEntites(db, "abe56e1b444ef4f637971dfdf5c14ce1", 0, 0)
	for _, entity := range movieEntities {
		movieEntry := &pb.MovieEntry{
			Version:     1,
			Id:          "abcdefghijkl",
			ChannelId:   entity.channelID,
			TopicId:     entity.topicID,
			Title:       entity.title,
			PublishedAt: entity.publishedAt.Unix(),
			Url:         entity.url,
			Size:        entity.size,
			Descr:       entity.descr,
			Geo:         entity.geo,
			IsNew:       entity.isNew,
		}

		duration, err := time.Parse("15:04:05", entity.duration)
		if err == nil {
			movieEntry.Duration = int64(duration.Second() + (duration.Minute() * 60) + (duration.Hour() * 3600))
		}

		if entity.websiteURL != "" {
			movieEntry.HasWebsiteUrl = true
		}
		if entity.subTitleURL != "" {
			movieEntry.HasSubtitleUrl = true
		}
		if entity.hdFormatURL != "" {
			movieEntry.HasHdFormatUrl = true
		}
		if entity.historyURL != "" {
			movieEntry.HasHistoryUrl = true
		}

		catalog.Movies = append(catalog.Movies, movieEntry)
	}

	// Write the new address book back to disk.
	out, err := proto.Marshal(catalog)
	if err != nil {
		log.Fatalln("Failed to encode movie catalog:", err)
	}
	if err := ioutil.WriteFile("moviecat.dat", out, 0644); err != nil {
		log.Fatalln("Failed to write moviecat.dat:", err)
	}
}

func findAllMovieEntites(db *sql.DB, schema string, limit, offset int) ([]movieEntity, error) {
	var result []movieEntity

	sqlStmt := fmt.Sprintf(
		`SELECT id, channel_id, topic_id, title, published_at, duration, size, 
            descr, url, website_url, sub_title_url, small_format_url, 
            hd_format_url, unix_date, history_url, geo, is_new 
        FROM %s.movies`, schema)

	if limit > 0 {
		sqlStmt = fmt.Sprintf("%s LIMIT %d", sqlStmt, limit)
	}

	if offset > 0 {
		sqlStmt = fmt.Sprintf("%s OFFSET %d", sqlStmt, offset)
	}

	rows, err := db.Query(sqlStmt)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var entity movieEntity
		if err := rows.Scan(&entity.id, &entity.channelID, &entity.topicID,
			&entity.title, &entity.publishedAt, &entity.duration, &entity.size,
			&entity.descr, &entity.url, &entity.websiteURL, &entity.subTitleURL,
			&entity.smallFormatURL, &entity.hdFormatURL, &entity.unixDate,
			&entity.historyURL, &entity.geo, &entity.isNew); err != nil {
			return nil, err
		}

		result = append(result, entity)
	}

	return result, nil
}

func findAllChannels(db *sql.DB, schema string) (map[int64]string, error) {
	var result map[int64]string

	result = make(map[int64]string)
	sqlStmt := fmt.Sprintf("SELECT id, name FROM %s.channels", schema)

	rows, err := db.Query(sqlStmt)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}

		result[id] = name
	}

	return result, nil
}

func findAllTopics(db *sql.DB, schema string) (map[int64]string, error) {
	var result map[int64]string

	result = make(map[int64]string)
	sqlStmt := fmt.Sprintf("SELECT id, name FROM %s.topics", schema)

	rows, err := db.Query(sqlStmt)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}

		result[id] = name
	}

	return result, nil
}
