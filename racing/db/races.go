package db

import (
	"database/sql"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"

	"git.neds.sh/matty/entain/racing/proto/racing"
)

// RacesRepo provides repository access to races.
type RacesRepo interface {
	// Init will initialise our races repository.
	Init() error

	// List will return a list of races.
	List(filter *racing.ListRacesRequestFilter) ([]*racing.Race, error)

	// Get single race by Id
	GetRaceById(id string) (*racing.Race, error)
}

type racesRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewRacesRepo creates a new races repository.
func NewRacesRepo(db *sql.DB) RacesRepo {
	return &racesRepo{db: db}
}

// Init prepares the race repository dummy data.
func (r *racesRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy races.
		err = r.seed()
	})

	return err
}

func (r *racesRepo) List(filter *racing.ListRacesRequestFilter) ([]*racing.Race, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getRaceQueries()[racesList]

	query, args = r.applyFilter(query, filter)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanRaces(rows)
}

func (r *racesRepo) applyFilter(query string, filter *racing.ListRacesRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

	if len(filter.MeetingIds) > 0 {
		clauses = append(clauses, "meeting_id IN ("+strings.Repeat("?,", len(filter.MeetingIds)-1)+"?)")

		for _, meetingID := range filter.MeetingIds {
			args = append(args, meetingID)
		}
	}

	// add visible filter
	if filter.IsVisible {
		clauses = append(clauses, "visible IS TRUE")
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	// add order_by filter
	switch filter.OrderBy {
	case "id":
		if filter.IsDesc {
			query += " ORDER BY id DESC"
		} else {
			query += " ORDER BY id"
		}
	case "name":
		if filter.IsDesc {
			query += " ORDER BY name DESC"
		} else {
			query += " ORDER BY name"
		}
	case "meeting_id":
		if filter.IsDesc {
			query += " ORDER BY meeting_id DESC"
		} else {
			query += " ORDER BY meeting_id"
		}
	case "number":
		if filter.IsDesc {
			query += " ORDER BY number DESC"
		} else {
			query += " ORDER BY number"
		}
	case "visible":
		if filter.IsDesc {
			query += " ORDER BY visible DESC"
		} else {
			query += " ORDER BY visible"
		}
	case "advertised_start_time":
		if filter.IsDesc {
			query += " ORDER BY advertised_start_time DESC"
		} else {
			query += " ORDER BY advertised_start_time"
		}
	}

	return query, args
}

func (m *racesRepo) scanRaces(
	rows *sql.Rows,
) ([]*racing.Race, error) {
	var races []*racing.Race

	for rows.Next() {
		var race racing.Race
		var advertisedStart time.Time

		if err := rows.Scan(&race.Id, &race.MeetingId, &race.Name, &race.Number, &race.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		race.AdvertisedStartTime = ts

		// Add status field in response race
		if ts.AsTime().Before(time.Now()) {
			race.Status = "CLOSED"
		} else {
			race.Status = "OPEN"
		}

		races = append(races, &race)
	}

	return races, nil
}

func (r *racesRepo) GetRaceById(id string) (*racing.Race, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getRaceQueries()[racesList]

	query += " WHERE id = " + id

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	refactorRows, err := r.scanRaces((rows))

	if err != nil {
		return nil, err
	}

	if len(refactorRows) == 0 {
		return nil, nil
	}

	return refactorRows[0], nil
}
