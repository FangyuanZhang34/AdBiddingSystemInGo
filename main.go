package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
)

// Advertiser type
type Advertiser struct {
	AdvertiserID int     `json:"advertiser_id"`
	Name         string  `json:"name"`
	Budget       float64 `json:"budget"`
}

// Ad type
type Ad struct {
	AdID         string  `json:"ad_id"`
	Bid          float64 `json:"bid"`
	ImageURL     string  `json:"image_url"`
	AdvertiserID int     `json:"advertiser_id"`
	AdScore      float64 `json:"ad_score"`
}

/*
return:
	ad exists: true, nil
	ad not exists: false, nil
	other error: false, err
*/
func checkAdvertiserExists(db *sql.DB, advertiser Advertiser) (bool, error) {
	var name string
	err := db.QueryRow("SELECT name FROM advertiser WHERE name = ?", advertiser.Name).Scan(&name)
	// fail to select
	if err != nil && err != sql.ErrNoRows {
		return false, errors.New("Failed to SELECT from advertiser table")
	}
	// not exist
	if err == sql.ErrNoRows {
		return false, nil
	}
	// exist
	return true, nil
}

/*
use "checkAdvertiserExists" to check if the advertiser already exists
then, insert an advertiser into advertiser table
return:
	err
	nil
*/
func insertAdvertiser(advertiser Advertiser) error {
	db, err := sql.Open("mysql", "root:root@(localhost:3306)/AdSysGo")
	if err != nil {
		return errors.New("Failed to connect to MySQL database")
	}
	defer db.Close()

	// check if the advertiser already exists
	exists, err := checkAdvertiserExists(db, advertiser)
	if err != nil {
		return errors.New("Failed to select from advertiser table")
	}
	if exists {
		return errors.New("Advertiser already exists")
	}

	// if not exist, insert the advertiser into advertiser table
	insert, err := db.Query("INSERT INTO advertiser (name, budget) VALUES(?, ?)", advertiser.Name, advertiser.Budget)
	if err != nil {
		return errors.New("Failed to insert into advertiser table")
	}
	defer insert.Close()

	return nil
}

/*
HanldeFunction
use "insertAdvertiser" to add a row of an advertiser into advertiser table
*/
func handleFuncAddAdvertiser(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one advertiser insertion request")
	w.Header().Set("Content-Type", "text/plain")

	if req.Method != "POST" {
		return
	}

	// decode the json format info into Advertiser type
	decoder := json.NewDecoder(req.Body)
	var advertiser Advertiser
	if err := decoder.Decode(&advertiser); err != nil {
		http.Error(w, "Cannot decode advertiser's data from client", 400)
		fmt.Println("Cannot decode advertiser's data from client.", err)
		return
	}
	// insert advertiser into advertiser table
	if err := insertAdvertiser(advertiser); err != nil {
		if err.Error() == "Failed to connect MySQL database" {
			http.Error(w, "Failed to connect MySQL database.", 500)
		} else if err.Error() == "Failed to select from advertiser table" {
			http.Error(w, "Failed to select from advertiser table.", 500)
		} else if err.Error() == "Advertiser already exists" {
			http.Error(w, "Advertiser already exists.", 400)
		} else if err.Error() == "Failed to insert into advertiser table" {
			http.Error(w, "Failed to insert into advertiser table.", 500)
		}
		return
	}
	w.Write([]byte("Advertiser added successfully"))

}

/*
add an ad into ad table
return:
	error
	nil
*/
func insertAd(ad Ad) error {
	db, err := sql.Open("mysql", "root:root@(localhost:3306)/AdSysGo")
	if err != nil {
		return errors.New("Failed to connect MySQL database")
	}

	insert, err := db.Query("INSERT INTO ad (bid, image_url, advertiser_id, ad_score) VALUES (?, ?, ?, ?)", ad.Bid, ad.ImageURL, ad.AdvertiserID, ad.AdScore)
	if err != nil {
		return errors.New("Failed to add into ad table")
	}
	defer insert.Close()

	return nil
}

/*
HanldeFunction
use "insertAd" to add an ad into ad table
*/
func handleFuncAddAd(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one ad insertion request")
	w.Header().Set("Content-Type", "text/plain")

	if req.Method != "POST" {
		return
	}

	// decode the json format info into Ad type
	decoder := json.NewDecoder(req.Body)
	var ad Ad
	if err := decoder.Decode(&ad); err != nil {
		http.Error(w, "Cannot decode ad's data from client", 400)
		fmt.Println("Cannot decode ad's data from client.", err)
		return
	}
	// insert ad into ad table
	if err := insertAd(ad); err != nil {
		http.Error(w, "Failed to add advertisement data into ad table.", 400)
		return
	}
	w.Write([]byte("Ad added successfully"))

}

/*
select all ads from database
convert each ad into type Ad
return:
	a slice of type Ad
*/
func selectAllAds() ([]Ad, error) {
	// connect to MySQL database
	db, err := sql.Open("mysql", "root:root@(localhost:3306)/AdSysGo")
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
	db, err := sql.Open("mysql", "root:root@(localhost:3306)/AdSysGo")
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
			http.Error(w, "Failed to convert MySQL data into Ad type", 500)
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

	http.ListenAndServe("localhost:8080", nil)
}
