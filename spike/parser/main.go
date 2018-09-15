package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	pq "github.com/lib/pq"
)

const (
	colChannel        = 0
	colTopic          = 1
	colTitle          = 2
	colDate           = 3
	colTime           = 4
	colDuration       = 5
	colSize           = 6
	colDescr          = 7
	colURL            = 8
	colWebsiteURL     = 9
	colSubTitleURL    = 10
	colSmallFormatURL = 12
	colHDFormatURL    = 14
	colUnixDate       = 16
	colHistoryURL     = 17
	colGeo            = 18
	colIsNew          = 19
)

type movieEntry struct {
	channel        string
	topic          string
	title          string
	publishedAt    time.Time
	duration       string
	size           uint64
	descr          string
	url            string
	websiteURL     string
	subTitleURL    string
	smallFormatURL string
	hdFormatURL    string
	unixDate       uint64
	historyURL     string
	geo            string
	isNew          bool
}

func main() {
	fmt.Println("Starting MV parser")

	connStr := "postgres://u4mdthk:pw4mdthk@localhost/mdthk?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	b, err := ioutil.ReadFile("filme.json")
	if err != nil {
		fmt.Print(err)
		return
	}

	str := string(b)
	result, _ := unmarshalMovieEntries(str)

	fmt.Printf("Entries: %d\n", len(result))

	err = bulkCopyMovieEntries(db, result)
	if err != nil {
		fmt.Print(err)
	}
}

func extractMovieEntryKeyIndices(str string) []int {
	var result []int
	key := "\"X\""

	i := strings.Index(str, key)
	for i != -1 {
		s := str[i:]
		if isMovieEntryKey(s) {
			result = append(result, i)
		}
		j := strings.Index(s[1:], key)
		if j == -1 {
			i = -1
		} else {
			i += j + 1
		}
	}

	return result
}

func isMovieEntryKey(str string) bool {
	// Strip the JSON key and trim
	s := strings.Trim(str[3:], " \n\r\t")

	// Check if colon exists
	if s[:1] != ":" {
		return false
	}

	// Strip the colon and trim
	s = strings.Trim(s[1:], " \n\r\t")

	// Check if opening squared bracket exists
	if s[:1] != "[" {
		return false
	}

	return true
}

func extractMovieEntries(str string) ([]string, error) {
	var result []string

	indicies := extractMovieEntryKeyIndices(str)
	for n := 0; n < len(indicies); n++ {
		i := indicies[n]
		j := len(str)
		if n+1 < len(indicies) {
			j = indicies[n+1]
		}

		s := str[i:j]
		s = stripAndValidatePrefix(s)
		if s == "" {
			return nil, fmt.Errorf("Invalid movie list")
		}
		s = stripAndValidateSuffix(s)
		if s == "" {
			return nil, fmt.Errorf("Invalid movie list")
		}

		result = append(result, s)
	}

	return result, nil
}

// stripAndValidatePrefix checks if the movie entry starts with "X":[
// and ignores whitespaces. On success it returns the beginning of the
// JSON array. On error an empty string is returned.
func stripAndValidatePrefix(str string) string {
	// Strip the JSON key and trim
	result := strings.Trim(str[3:], " \n\r\t")

	// Check if colon exists
	if result[:1] != ":" {
		return ""
	}

	// Strip the colon and trim
	result = strings.Trim(result[1:], " \n\r\t")

	// Check if opening squared bracket exists
	if result[:1] != "[" {
		return ""
	}

	return result
}

func stripAndValidateSuffix(str string) string {
	result := strings.Trim(str, " \n\r\t")

	if result[len(result)-1:] == "}" {
		result = strings.Trim(result[:len(result)-1], " \n\r\t")
	}

	if result[len(result)-1:] == "," {
		result = strings.Trim(result[:len(result)-1], " \n\r\t")
	}

	if result[len(result)-1:] != "]" {
		return ""
	}

	return result
}

func unmarshalMovieEntries(str string) ([]movieEntry, error) {
	var result []movieEntry

	entries, err := extractMovieEntries(str)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		var vals []interface{}

		err := json.Unmarshal([]byte(e), &vals)
		if err != nil {
			return nil, err
		}
		result = append(result, buildMovieEntry(vals))
	}

	populateEmptyFields(&result)

	return result, nil
}

func populateEmptyFields(entries *[]movieEntry) {
	var channel, topic string

	for i := 0; i < len(*entries); i++ {
		if (*entries)[i].channel == "" {
			(*entries)[i].channel = channel
		} else {
			channel = (*entries)[i].channel
		}

		if (*entries)[i].topic == "" {
			(*entries)[i].topic = topic
		} else {
			topic = (*entries)[i].topic
		}
	}
}

func buildMovieEntry(vals []interface{}) movieEntry {
	var result movieEntry
	var dt, tm string

	for i, v := range vals {
		switch i {
		case colChannel:
			result.channel = strings.Trim(v.(string), " ")
			break
		case colTopic:
			result.topic = strings.Trim(v.(string), " ")
			break
		case colTitle:
			result.title = strings.Trim(v.(string), " ")
			break
		case colDate:
			dt = strings.Trim(v.(string), " ")
			break
		case colTime:
			tm = strings.Trim(v.(string), " ")
		case colDuration:
			result.duration = strings.Trim(v.(string), " ")
			break
		case colSize:
			size, err := strconv.ParseUint(strings.Trim(v.(string), " "), 10, 64)
			if err == nil {
				result.size = size
			}
			break
		case colDescr:
			result.descr = strings.Trim(v.(string), " ")
			break
		case colURL:
			result.url = strings.Trim(v.(string), " ")
			break
		case colWebsiteURL:
			result.websiteURL = strings.Trim(v.(string), " ")
			break
		case colSubTitleURL:
			result.subTitleURL = convertToFullURL(result.url, v.(string))
			break
		case colSmallFormatURL:
			result.smallFormatURL = convertToFullURL(result.url, v.(string))
			break
		case colHDFormatURL:
			result.hdFormatURL = convertToFullURL(result.url, v.(string))
			break
		case colUnixDate:
			unixDate, err := strconv.ParseUint(strings.Trim(v.(string), " "), 10, 64)
			if err == nil {
				result.unixDate = unixDate
			}
			break
		case colHistoryURL:
			result.historyURL = convertToFullURL(result.url, v.(string))
			break
		case colGeo:
			result.geo = strings.Trim(v.(string), " ")
			break
		case colIsNew:
			isNew, err := strconv.ParseBool(strings.Trim(v.(string), " "))
			if err == nil {
				result.isNew = isNew
			}
			break
		}
	}

	publishedAt, err := time.Parse("02.01.2006 15:04", dt+" "+tm)
	if err == nil {
		result.publishedAt = publishedAt
	}

	return result
}

func convertToFullURL(baseURL, url string) string {
	var result = strings.Trim(url, " ")
	if result == "" || baseURL == "" {
		return result
	}

	// Check if there's a dash with an index, otherwise return the given URL
	i := strings.Index(result, "|")
	if i == -1 {
		return result
	}

	// Get the index for the baseURL
	j, err := strconv.Atoi(url[:i])
	if err != nil {
		return ""
	}
	if j >= len(baseURL) {
		return ""
	}

	// Get URL path until index
	result = baseURL[:j] + url[i+1:]

	return result
}

func bulkCopyMovieEntries(db *sql.DB, entries []movieEntry) error {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("abe56e1b444ef4f637971dfdf5c14ce1",
		"movies", "channel", "channel_id", "topic", "topic_id", "title",
		"published_at", "duration", "size", "descr", "url", "website_url", "sub_title_url",
		"small_format_url", "hd_format_url", "unix_date", "history_url", "geo", "is_new"))
	if err != nil {
		return err
	}

	for _, entry := range entries {
		_, err = stmt.Exec(entry.channel, nil, entry.topic, nil, entry.title, entry.publishedAt,
			entry.duration, entry.size, entry.descr, entry.url, entry.websiteURL, entry.subTitleURL,
			entry.smallFormatURL, entry.hdFormatURL, entry.unixDate, entry.historyURL, entry.geo, entry.isNew)
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

// GenerateRandomBytes returns securely generated random bytes.
// It will return an error if the system's secure random
// number generator fails to function correctly, in which
// case the caller should not continue.
func generateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

// GenerateRandomString returns a URL-safe, base64 encoded
// securely generated random string.
// It will return an error if the system's secure random
// number generator fails to function correctly, in which
// case the caller should not continue.
func generateRandomString(s int) (string, error) {
	b, err := generateRandomBytes(s)
	r := base64.URLEncoding.EncodeToString(b)
	return r, err
}

func createUniqueID(idMap map[string]bool) string {
	var id string
	var err error
	exists := true
	for exists == true {
		id, err = generateRandomString(8)
		if err != nil {
			log.Fatal(err)
		}
		_, exists = idMap[id[:len(id)-1]]
	}
	return id[:len(id)-1]
}
