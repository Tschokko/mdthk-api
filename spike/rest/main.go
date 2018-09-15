package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/urfave/negroni"
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

type movieMetaResource struct {
	PublishedAt   int64  `json:"publishedAt"`
	Version       int    `json:"version"`
	MD5Hash       string `json:"hash"`
	ChannelsCount int    `json:"channelsCount"`
	TopicsCount   int    `json:"topicsCount"`
	MoviesCount   int    `json:"moviesCount"`
}

type movieResource struct {
	Slug              string `json:"id"`
	ChannelID         int64  `json:"ch,omitempty"`
	TopicID           int64  `json:"tp,omitempty"`
	Title             string `json:"ti,omitempty"`
	PublishedAt       int64  `json:"ts,omitempty"`
	Duration          int64  `json:"dr,omitempty"`
	Size              int64  `json:"sz,omitempty"`
	Descr             string `json:"ds,omitempty"`
	HasWebsiteURL     bool   `json:"ws,omitempty"`
	HasSubTitleURL    bool   `json:"st,omitempty"`
	HasSmallFormatURL bool   `json:"sm,omitempty"`
	HasHDFormatURL    bool   `json:"hd,omitempty"`
	HasHistoryURL     bool   `json:"hi,omitempty"`
	Geo               string `json:"ge,omitempty"`
	IsNew             bool   `json:"ne,omitempty"`
}

type movieListResource struct {
	Meta     movieMetaResource `json:"meta"`
	Channels map[int64]string  `json:"channels,omitempty"`
	Topics   map[int64]string  `json:"topics,omitempty"`
	Movies   []movieResource   `json:"movies,omitempty"`
}

type service struct {
	r  *mux.Router
	db *sql.DB
}

func main() {
	fmt.Println("Starting MV Service")

	connStr := "postgres://u4mdthk:pw4mdthk@localhost/mdthk?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	r := mux.NewRouter()
	newService(r, db)
	n := negroni.Classic() // Includes some default middlewares
	n.UseHandler(r)

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: n,
	}

	log.Fatal(httpServer.ListenAndServe())
}

func countMovieEntities(db *sql.DB, schema string) (int, error) {
	result := 0

	sqlStmt := fmt.Sprintf("SELECT COUNT(id) FROM %s.movies", schema)
	err := db.QueryRow(sqlStmt).Scan(&result)
	if err != nil {
		return 0, err
	}

	return result, nil
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

// NewService creates a new auto update service instance
func newService(r *mux.Router, db *sql.DB) *service {
	s := &service{
		r:  r,
		db: db,
	}
	s.setupHandleFuncs()
	return s
}

func (svc *service) setupHandleFuncs() {
	//svc.r.Use(middleware)
	svc.r.HandleFunc("/", svc.handleIndex).Methods("GET")
	svc.r.HandleFunc("/movies", svc.handleMovies).Methods("GET")
	// svc.r.Handle("/movies",
	// 	gziphandler.GzipHandler(http.HandlerFunc(svc.handleMovies))).Methods("GET")
}

func (svc *service) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain")

	fmt.Fprintf(w, "OK")
}

func (svc *service) handleMovies(w http.ResponseWriter, r *http.Request) {
	var resource movieListResource
	var err error

	etag := "abe56e1b444ef4f637971dfdf5c14ce1"
	schema := "abe56e1b444ef4f637971dfdf5c14ce1"
	limit := 0
	offset := 0
	queryParams := r.URL.Query()

	if val, ok := queryParams["limit"]; ok {
		limit, err = strconv.Atoi(val[0])
		if err != nil {
			limit = 0
		}
	}
	if val, ok := queryParams["offset"]; ok {
		offset, err = strconv.Atoi(val[0])
		if err != nil {
			offset = 0
		}
	}

	// E-Tag handling
	if match := r.Header.Get("If-None-Match"); match != "" {
		if strings.Contains(match, etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// Populate meta
	resource.Meta.PublishedAt = time.Now().Unix()
	resource.Meta.Version = 3
	resource.Meta.MD5Hash = schema                                    // This is test code!
	resource.Meta.MoviesCount, _ = countMovieEntities(svc.db, schema) // TODO: Add error handling!

	// Populate channels if nochannels not set
	if _, ok := queryParams["nochannels"]; !ok {
		resource.Channels, _ = findAllChannels(svc.db, schema)
	}

	// Populate topics if notopics not set
	if _, ok := queryParams["notopics"]; !ok {
		resource.Topics, _ = findAllTopics(svc.db, schema)
	}

	// Populate movies if nomovies not set
	if _, ok := queryParams["nomovies"]; !ok {
		entities, _ := findAllMovieEntites(svc.db, schema, limit, offset)
		for _, entity := range entities {
			resource.Movies = append(resource.Movies, movieEntityToResource(entity))
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Etag", etag)
	w.Header().Set("Cache-Control", "max-age=7200") // 2 hours = 7200

	json.NewEncoder(w).Encode(resource)
}

func movieEntityToResource(entity movieEntity) movieResource {
	var result movieResource

	result.Slug = "abcdefghijkl"
	result.ChannelID = entity.channelID
	result.TopicID = entity.topicID
	result.Title = entity.title
	result.PublishedAt = entity.publishedAt.Unix()
	duration, err := time.Parse("15:04:05", entity.duration)
	if err == nil {
		result.Duration = int64(duration.Second() + (duration.Minute() * 60) + (duration.Hour() * 3600))
	}
	result.Size = entity.size
	result.Descr = entity.descr
	if entity.websiteURL != "" {
		result.HasWebsiteURL = true
	}
	if entity.subTitleURL != "" {
		result.HasSubTitleURL = true
	}
	if entity.hdFormatURL != "" {
		result.HasHDFormatURL = true
	}
	if entity.historyURL != "" {
		result.HasHistoryURL = true
	}
	result.Geo = entity.geo
	result.IsNew = entity.isNew

	return result
}
