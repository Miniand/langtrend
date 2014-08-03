package github

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	DateFormat = "2006-01-02"
)

type searchResult struct {
	TotalCount        int  `json:"total_count"`
	IncompleteResults bool `json:"incomplete_results"`
}

func GetCreatedOnDateForLang(date time.Time, lang, username, password string) (int, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET",
		fmt.Sprintf("https://api.github.com/search/repositories?q=%s",
			url.QueryEscape(fmt.Sprintf(`language:"%s" created:%s`,
				lang, Format(date)))), nil)
	if err != nil {
		return 0, err
	}
	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	res := searchResult{}
	if err := decoder.Decode(&res); err != nil {
		return 0, err
	}
	if res.IncompleteResults {
		return 0, errors.New("did not get complete results")
	}
	return res.TotalCount, nil
}

func Format(date time.Time) string {
	return date.Format(DateFormat)
}
