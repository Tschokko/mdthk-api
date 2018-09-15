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
	colMetaDataPublishedAt = 1
	colMetaDataVersion     = 2
	colMetaDataMD5Hash     = 4
	colChannel             = 0
	colTopic               = 1
	colTitle               = 2
	colDate                = 3
	colTime                = 4
	colDuration            = 5
	colSize                = 6
	colDescr               = 7
	colURL                 = 8
	colWebsiteURL          = 9
	colSubTitleURL         = 10
	colSmallFormatURL      = 12
	colHDFormatURL         = 14
	colUnixDate            = 16
	colHistoryURL          = 17
	colGeo                 = 18
	colIsNew               = 19
)

type metaDataEntry struct {
	publishedAt time.Time
	version     string
	md5Hash     string
}

type movieEntry struct {
	channel        string
	channelID      int64
	topic          string
	topicID        int64
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

	b, err := ioutil.ReadFile("/Users/tlx3m3j/go/src/github.com/tschokko/mdthk-api/data/test1.json")
	if err != nil {
		log.Fatal(err)
	}

	str := string(b)
	meta, err := unmarshalMetaDataEntry(str)

	fmt.Printf("Meta: %v", meta)

	_, _, movies, _ := unmarshalMovieImportSource(str)

	fmt.Printf("Entries: %d\n", len(movies))

	err = bulkCopyMovieEntries(db, movies)
	if err != nil {
		fmt.Print(err)
	}
}

// unmarshalMetaDataEntry extracts the meta data entry of the import source.
// The meta data is stored inside the first "Filmliste" dict element.
func unmarshalMetaDataEntry(str string) (metaDataEntry, error) {
	result := metaDataEntry{}
	key := "\"Filmliste\""

	// Fetch first "Filmliste" position
	i := strings.Index(str, key)
	if i == -1 {
		return result, fmt.Errorf("could not find meta data")
	}

	s := str[i:]

	// Strip the JSON key and trim
	s = strings.Trim(s[len(key):], " \n\r\t")

	// Check if colon exists
	if s[:1] != ":" {
		return result, fmt.Errorf("unexpected movie list format")
	}

	// Strip the colon and trim
	s = strings.Trim(s[1:], " \n\r\t")

	// Check if opening squared bracket exists
	if s[:1] != "[" {
		return result, fmt.Errorf("unexpected movie list format")
	}

	// Fetch second "Filmliste" position
	j := strings.Index(s, key)
	if j == -1 {
		return result, fmt.Errorf("unexpected movie list format")
	}

	s = strings.Trim(s[:j], " \n\r\t")

	// Trim the colon if exists
	if s[len(s)-1:] == "," {
		s = strings.Trim(s[:len(s)-1], " \n\r\t")
	}

	// Check if closing squared bracket exists
	if s[len(s)-1:] != "]" {
		return result, fmt.Errorf("unexpected movie list format")
	}

	// Unmarshal the JSON array
	var vals []interface{}
	err := json.Unmarshal([]byte(s), &vals)
	if err != nil {
		return result, fmt.Errorf("failed to unmarshal meta data")
	}

	if len(vals) < 5 {
		return result, fmt.Errorf("unexpected meta data values")
	}

	// Parse the published at timestamp
	result.publishedAt, err = time.Parse("02.01.2006, 15:04",
		vals[colMetaDataPublishedAt].(string))
	if err != nil {
		return result, fmt.Errorf("invalied published at date in meta data")
	}

	result.version = strings.Trim(vals[colMetaDataVersion].(string), " ")
	if result.version == "" {
		return result, fmt.Errorf("empty version in meta data")
	}

	result.md5Hash = strings.Trim(vals[colMetaDataMD5Hash].(string), " ")
	if result.md5Hash == "" {
		return result, fmt.Errorf("empty md5 hash in meta data")
	}

	return result, nil
}

// extractMovieEntries searches the input source for valid movie entries. A
// movie entry corresponds an JSON dict element with key "X" and a value of
// type JSON array. E.g. "X": ["val1", "val2", ...]
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
			return nil, fmt.Errorf("invalid movie list")
		}
		s = stripAndValidateSuffix(s)
		if s == "" {
			return nil, fmt.Errorf("invalid movie list")
		}

		result = append(result, s)
	}

	return result, nil
}

// extractMovieEntryKeyIndices searches the input source for "X" strings.
// If the string is followed by a JSON array, we expect a proper movie entry.
// See also function isMovieEntryKey.
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

// isMovieEntryKey checks if the found "X" string entry is a dict key
// followed by an opening JSON array.
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

// stripAndValidatePrefix checks if the movie entry starts with "X":[
// and ignores whitespaces. On success it returns the beginning of an
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

// stripAndValidateSuffix checks if the movie entry ends with a closing squared
// bracket and ignores whitespaces. Each array is also comma seperated. The
// comma will be removed. If we reach the end of the import source, the closing
// curly bracket appears and will be removed, too. On success a proper parsable
// JSON array (as string) is returned.
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

// unmarshalMovieImportSource parses the import source for channel, topic and
// movie entries.
func unmarshalMovieImportSource(str string) (map[string]int64, map[string]int64, []movieEntry, error) {
	var result []movieEntry
	entries, err := extractMovieEntries(str)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, e := range entries {
		var vals []interface{}

		err := json.Unmarshal([]byte(e), &vals)
		if err != nil {
			return nil, nil, nil, err
		}
		result = append(result, buildMovieEntry(vals))
	}

	channels, topics := populateChannelsAndTopics(&result)

	return channels, topics, result, nil
}

// populateChannelsAndTopics iterates over all movie entries and populates empty
// channel or topic fields with a corresponding entry. The import source sets
// the channel or topic field only on the first record. The following entries
// are empty until a new channel or topic starts.
// Additionally we're creating a channel and topic map which contains a unique
// ID for each channel and topic. This ID is then applied to the movie entry,
// too. You can do that all with SQL operations, but applying all IDs in the
// database tooks more than 60s. This approach consumes only a few seconds.
func populateChannelsAndTopics(entries *[]movieEntry) (map[string]int64, map[string]int64) {
	var channel, topic string
	var channels map[string]int64
	var topics map[string]int64
	var channelsLastID, topicsLastID int64

	channels = make(map[string]int64)
	topics = make(map[string]int64)
	channelsLastID = 1
	topicsLastID = 1

	for i := 0; i < len(*entries); i++ {
		if (*entries)[i].channel == "" {
			(*entries)[i].channel = channel
		} else {
			channel = (*entries)[i].channel

			// Add new channel to map if not exists
			if _, ok := channels[channel]; !ok {
				channels[channel] = channelsLastID
				channelsLastID++
			}
		}

		if (*entries)[i].topic == "" {
			(*entries)[i].topic = topic
		} else {
			topic = (*entries)[i].topic

			// Add new topic to map if not exists
			if _, ok := topics[topic]; !ok {
				topics[topic] = topicsLastID
				topicsLastID++
			}
		}

		// Update channel and topic ID
		(*entries)[i].channelID = channels[(*entries)[i].channel]
		(*entries)[i].topicID = topics[(*entries)[i].topic]
	}

	return channels, topics
}

// buildMovieEntry creates a movieEntry from a list of values. The import source
// contains only JSON arrays for each movie. The order of the array elements
// matches a specified attribute.
// TODO: For backward compatibility observe the version in the meta data of the
// import source.
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

// convertToFullURL builds proper URLs from the given data in the import source.
// Most URLs in the import source are cutted down to the modified portion
// compared to the main / base URL, which is a full URL.
// E.g. the full base URL is "http://.../abc.mp4" and the cutted down HD format
// URL is "100|def.mp4". That means from position 100 in the base URL replace
// everything with the string given after the dash.
// If the non base URLs doesn't contain a dash, than a regular (full) URL is
// set or none.
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
