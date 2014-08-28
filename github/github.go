package github

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beefsack/go-rate"
)

const (
	DateFormat = "2006-01-02"
)

var rateLimiters = map[string]*rate.RateLimiter{}

func rateLimiter(name string) *rate.RateLimiter {
	rl, ok := rateLimiters[name]
	if !ok {
		limit := 20
		if name == "" {
			limit = 5
		}
		rl = rate.New(limit, time.Minute)
		rateLimiters[name] = rl
	}
	return rl
}

type SearchResult struct {
	TotalCount        int  `json:"total_count"`
	IncompleteResults bool `json:"incomplete_results"`
}

func ApiSearch(
	args map[string]string,
	username, password string,
) (SearchResult, error) {
	rateLimiter(username).Wait()
	sr := SearchResult{}
	client := &http.Client{}
	query := []string{}
	for arg, val := range args {
		query = append(query, fmt.Sprintf(`%s:"%s"`, arg, val))
	}
	req, err := http.NewRequest("GET",
		fmt.Sprintf("https://api.github.com/search/repositories?q=%s",
			url.QueryEscape(strings.Join(query, " "))), nil)
	if err != nil {
		return sr, err
	}
	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}
	resp, err := client.Do(req)
	if err != nil {
		return sr, err
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&sr)
	return sr, err
}

func GetCountOnDateForLang(
	date time.Time,
	kind, language, username, password string,
) (int, error) {
	sr, err := ApiSearch(map[string]string{
		"language": language,
		kind:       FormatDate(date),
	}, username, password)
	if err != nil {
		return 0, err
	}
	if sr.IncompleteResults {
		return 0, errors.New("got incomplete results")
	}
	return sr.TotalCount, nil
}

func FormatDate(date time.Time) string {
	return date.Format(DateFormat)
}

func ParseDate(date string) (time.Time, error) {
	return time.Parse(DateFormat, date)
}
