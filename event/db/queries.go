package db

const (
	eventList = "list"
)

func getEventQueries() map[string]string {
	return map[string]string{
		eventList: `
			SELECT 
				id, 
				name,
				venue, 
				online, 
				advertised_start_time,
				bonus
			FROM events
		`,
	}
}
