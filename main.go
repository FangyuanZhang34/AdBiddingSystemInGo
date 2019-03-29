package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlDataSourceName = "root:root@tcp(www.fyz34.com:9500)/ad_sys"
	// mysqlDataSourceName = "root:root@(localhost:3306)/AdSysGo"
)

// Advertiser type
type Advertiser struct {
	AdvertiserID int     `json:"advertiser_id"`
	Name         string  `json:"name"`
	Budget       float64 `json:"budget"`
}

// Ad type
type Ad struct {
	AdID         int     `json:"ad_id"`
	Bid          float64 `json:"bid"`
	ImageURL     string  `json:"image_url"`
	AdvertiserID int     `json:"advertiser_id"`
	AdScore      float64 `json:"ad_score"`
}

// AddBudgetProcess type
type AddBudgetProcess struct {
	AdvertiserID int     `json:"advertiser_id"`
	AddBudget    float64 `json:"add_budget"`
}

// SearchAdvertiserProcess type
type SearchAdvertiserProcess struct {
	Name string `json:"name"`
}

// SearchAdsByAdvertiserIDProcess type
type SearchAdsByAdvertiserIDProcess struct {
	AdvertiserID int `json:"advertiser_id"`
}

/*
select all ads from database
convert each ad into type Ad
return:
	a slice of type Ad
*/
func selectAllAds() ([]Ad, error) {
	// connect to MySQL database
	db, err := sql.Open("mysql", mysqlDataSourceName)
	if err != nil {
		return nil, errors.New("Failed to connect the database")
	}
	defer db.Close()

	// select all ads and save them into a slice of type Ad
	var Ads []Ad
	result, err := db.Query("SELECT * FROM ad")
	if err != nil {
		return nil, errors.New("Failed to select all the ads from MySQL database")
	}
	defer result.Close()

	for result.Next() {
		var ad Ad
		// deal with possible Null values from database
		// if null bid / score ==> 0
		// if null imageuURL ==> ""
		var nilURL []byte
		var nilBid, nilAdScore sql.NullFloat64
		err = result.Scan(&ad.AdID, &nilBid, &nilURL, &ad.AdvertiserID, &nilAdScore)
		if err != nil {
			return nil, errors.New("Failed to convert MySQL data into Ad type")
		}
		ad.ImageURL = string(nilURL)
		ad.Bid = nilBid.Float64
		ad.AdScore = nilAdScore.Float64
		Ads = append(Ads, ad)
	}
	return Ads, nil
}

/*
update the budget of the chosen advertiser
return
*/
func updateBudget(cost float64, advertiserID int) error {
	// connect to database
	db, err := sql.Open("mysql", mysqlDataSourceName)
	if err != nil {
		return errors.New("Failed to connect the database")
	}
	defer db.Close()

	// select advertiser with advertiserID
	sele, err := db.Query("SELECT budget FROM advertiser WHERE advertiser_id=(?)", advertiserID)
	if err != nil {
		return errors.New("Failed to select from advertiser table")
	}
	defer sele.Close()

	// get old budget from selected result
	var newBudget float64
	for sele.Next() {
		var nilBudget sql.NullFloat64
		if err = sele.Scan(&nilBudget); err != nil {
			return errors.New("Failed to get old budget")
		}
		newBudget = nilBudget.Float64 - cost
	}

	// update old budget with new budget
	update, err := db.Query("UPDATE advertiser SET budget=(?) WHERE advertiser_id=(?)", newBudget, advertiserID)
	if err != nil {
		return errors.New("Failed to update budget")
	}
	defer update.Close()

	return nil

}

/*
rank all ads and get the top two
use second-place ad to compute the cpc price of the top-ranked ad
update the budget of the advertiser
response the client with the chosen ad data
*/
func handleFuncChooseAd(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one request for choosing an ad")
	w.Header().Set("Content-Type", "text/plain")

	if req.Method != "GET" {
		return
	}

	// allAds : a slice of Ad type including all the ads
	allAds, err := selectAllAds()
	if err != nil {
		if err.Error() == "Failed to connect the database" {
			http.Error(w, "Failed to connect the database.", 500)
		} else if err.Error() == "Failed to select all the ads from MySQL database" {
			http.Error(w, "Failed to select all the ads from MySQL database.", 500)
		} else if err.Error() == "Failed to convert MySQL data into Ad type" {
			http.Error(w, "Failed to convert MySQL data into Ad type.", 500)
		}
		return
	}

	// select the top two ads (chosen by bid * adscore)
	if len(allAds) < 2 {
		http.Error(w, "No enough ads in database.", 400)
	}

	var ad1, ad2 Ad
	if allAds[0].Bid*allAds[0].AdScore >= allAds[1].Bid*allAds[1].AdScore {
		ad1, ad2 = allAds[0], allAds[1]
	} else {
		ad1, ad2 = allAds[1], allAds[0]
	}

	for i := 2; i < len(allAds); i++ {
		if allAds[i].Bid*allAds[i].AdScore > ad1.Bid*ad1.AdScore {
			ad1 = allAds[i]
		} else if allAds[i].Bid*allAds[i].AdScore > ad2.Bid*ad2.AdScore {
			ad2 = allAds[i]
		}
	}

	// compute CPC of first advertisement
	cost := ad2.Bid*ad2.AdScore/ad1.AdScore + 0.01
	// update budget of corresponding advertiser
	if err := updateBudget(cost, ad1.AdvertiserID); err != nil {
		if err.Error() == "Failed to connect the database" {
			http.Error(w, "Failed to connect the database.", 500)
		} else if err.Error() == "Failed to select from advertiser table" {
			http.Error(w, "Failed to select from advertiser table.", 500)
		} else if err.Error() == "Failed to get old budget" {
			http.Error(w, "Failed to get old budget.", 500)
		} else if err.Error() == "Failed to update budget" {
			http.Error(w, "Failed to update budget.", 500)
		}
	}

	// convert chosen ad data into Json format
	topAdJSON, err := json.Marshal(ad1)
	if err != nil {
		http.Error(w, "Failed to parse allAds into JSON format", 500)
		fmt.Printf("Failed to parse allAds into JSON format %v.\n", err)
		return
	}

	// write the chosen ad json to response
	w.Write(topAdJSON)

}

func main() {
	fmt.Println("Start Ad System")

	// handler1: post: add advertiser into db
	http.HandleFunc("/addAdvertiser", handleFuncAddAdvertiser)
	// handler2: post: add ad into db
	http.HandleFunc("/addAd", handleFuncAddAd)
	// handler3: get: retrieve top ranked ad from db and update dudget of the advertiser
	http.HandleFunc("/chooseAd", handleFuncChooseAd)
	// handler4: post: add budget of an advertiser
	http.HandleFunc("/addBudget", handleFuncAddBudget)
	// handler5: post: search an advertiser with name
	http.HandleFunc("/searchAdvertiser", handleFuncSearchAdvertiser)
	// handler6: post: select all ads with with advertiser_id
	http.HandleFunc("/searchAdsByAdvertiserID", handleFuncSearchAdsByAdvertiserID)
	// handler7: post: delete an ad with ad_id
	http.HandleFunc("/deleteAd", handleFuncDeleteAd)

	http.ListenAndServe("localhost:8080", nil)
}
