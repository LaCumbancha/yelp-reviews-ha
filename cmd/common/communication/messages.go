package communication

// Loaded review.
type FullReview struct {
	ReviewId 		string 					`json:"review_id",omitempty`
	UserId 			string 					`json:"user_id",omitempty`
	BusinessId 		string 					`json:"business_id",omitempty`
	Stars 			float32 				`json:"stars",omitempty`
	Useful			int 					`json:"useful",omitempty`
	Funny 			int 					`json:"funny",omitempty`
	Cool			int 					`json:"cool",omitempty`
	Text 			string 					`json:"text",omitempty`
	Date 			string 					`json:"date",omitempty`
}

// Loaded business.
type FullBusiness struct {
	BusinessId 		string 					`json:"business_id",omitempty`
	Name 			string 					`json:"name",omitempty`
	Address 		string 					`json:"address",omitempty`
	City 			string 					`json:"city",omitempty`
	State			string 					`json:"state",omitempty`
	PostalCode 		string 					`json:"postal_code",omitempty`
	Latitude		float32 				`json:"latitude",omitempty`
	Longitude 		float32 				`json:"longitude",omitempty`
	Stars 			float32 				`json:"stars",omitempty`
	ReviewCount		int 					`json:"review_count",omitempty`
	IsOpen			int 					`json:"is_open",omitempty`
	Attributes		map[string]string		`json:"attributes",omitempty`
	Categories		string 					`json:"categories",omitempty`
	Hours			map[string]string		`json:"hours",omitempty`
}

// Funniest Cities flow.
type FunnyBusinessData struct {
	BusinessId 		string 					`json:"business_id",omitempty`
	Funny 			int 					`json:"funny",omitempty`
}

type CityBusinessData struct {
	BusinessId 		string 					`json:"business_id",omitempty`
	City 			string 					`json:"city",omitempty`
}

type FunnyCityData struct {
	City 			string 					`json:"city",omitempty`
	Funny 			int 					`json:"funny",omitempty`
}

// Weekday Histogram flow.
type WeekdayData struct {
	Weekday 		string 					`json:"weekday",omitempty`
	Reviews 		int 					`json:"reviews",omitempty`
}

// Top-Users flow.
type UserData struct {
	UserId 			string 					`json:"user_id",omitempty`
	Reviews			int 					`json:"reviews",omitempty`
}

// Bot-Users flow.
type HashedTextData struct {
	UserId 			string 					`json:"user_id",omitempty`
	HashedText		string 					`json:"hashed_text",omitempty`
}

// Best-Users flow.
type StarsData struct {
	UserId 			string 					`json:"user_id",omitempty`
	Stars			float32 				`json:"stars",omitempty`
}
