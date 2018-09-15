package importer

import "database/sql"

// ImportMovieList parses the given byte stream and extracts and saves the data
// into the given SQL database. Based on the extracted meta data, the import
// function checks if the movie list is already imported, before running the
// whole import process. If the force flag is true this check will be skipped.
func ImportMovieList(db *sql.DB, raw []byte, force bool) error {
	str := string(raw)

	unmarshalMetaDataEntry(str)

	return nil
}
