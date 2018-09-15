DROP SCHEMA IF EXISTS abe56e1b444ef4f637971dfdf5c14ce1
CASCADE;
CREATE SCHEMA abe56e1b444ef4f637971dfdf5c14ce1;

CREATE SEQUENCE abe56e1b444ef4f637971dfdf5c14ce1.channels_seq;
CREATE TABLE abe56e1b444ef4f637971dfdf5c14ce1.channels
(
    id bigint NOT NULL PRIMARY KEY DEFAULT nextval('abe56e1b444ef4f637971dfdf5c14ce1.channels_seq'),
    name text
);
ALTER SEQUENCE abe56e1b444ef4f637971dfdf5c14ce1.channels_seq
OWNED BY abe56e1b444ef4f637971dfdf5c14ce1.channels.id;

CREATE SEQUENCE abe56e1b444ef4f637971dfdf5c14ce1.topics_seq;
CREATE TABLE abe56e1b444ef4f637971dfdf5c14ce1.topics
(
    id bigint NOT NULL PRIMARY KEY DEFAULT nextval('abe56e1b444ef4f637971dfdf5c14ce1.topics_seq'),
    name text
);
ALTER SEQUENCE abe56e1b444ef4f637971dfdf5c14ce1.channels_seq
OWNED BY abe56e1b444ef4f637971dfdf5c14ce1.topics.id;

CREATE SEQUENCE abe56e1b444ef4f637971dfdf5c14ce1.movies_seq;
CREATE TABLE abe56e1b444ef4f637971dfdf5c14ce1.movies
(
    id bigint NOT NULL PRIMARY KEY DEFAULT nextval('abe56e1b444ef4f637971dfdf5c14ce1.movies_seq'),
    channel text,
    channel_id bigint REFERENCES abe56e1b444ef4f637971dfdf5c14ce1.channels,
    topic text,
    topic_id bigint REFERENCES abe56e1b444ef4f637971dfdf5c14ce1.topics,
    title text,
    published_at timestamp,
    duration varchar(10),
    size bigint,
    descr text,
    url varchar(2047),
    website_url varchar(2047),
    sub_title_url varchar(2047),
    small_format_url varchar(2047),
    hd_format_url varchar(2047),
    unix_date bigint,
    history_url varchar(2047),
    geo varchar(100),
    is_new bool
);
ALTER SEQUENCE abe56e1b444ef4f637971dfdf5c14ce1.channels_seq
OWNED BY abe56e1b444ef4f637971dfdf5c14ce1.movies.id;

INSERT INTO abe56e1b444ef4f637971dfdf5c14ce1.channels
    (name)
SELECT DISTINCT channel
FROM abe56e1b444ef4f637971dfdf5c14ce1.movies;

INSERT INTO abe56e1b444ef4f637971dfdf5c14ce1.topics
    (name)
SELECT DISTINCT topic
FROM abe56e1b444ef4f637971dfdf5c14ce1.movies;

UPDATE abe56e1b444ef4f637971dfdf5c14ce1.movies 
SET channel_id = (SELECT id FROM abe56e1b444ef4f637971dfdf5c14ce1.channels
     WHERE abe56e1b444ef4f637971dfdf5c14ce1.channels.name
		 = abe56e1b444ef4f637971dfdf5c14ce1.movies.channel);

         