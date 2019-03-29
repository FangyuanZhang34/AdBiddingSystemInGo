package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

/*
add an ad into ad table
return:
	error
	nil
*/
func insertAd(ad Ad) error {
	db, err := sql.Open("mysql", mysqlDataSourceName)
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

func selectAllAdsByAdvertiserID(id int) ([]Ad, error) {
	// connect to database
	db, err := sql.Open("mysql", mysqlDataSourceName)
	if err != nil {
		return nil, errors.New("Failed to connect MySQL database")
	}

	// select all ads with advertiser id and save them into a slice of type Ad
	var Ads []Ad
	result, err := db.Query("SELECT * FROM ad WHERE advertiser_id = ?", id)
	if err != nil {
		return nil, errors.New("Failed to select ads by advertiser_id from MySQL database")
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

func handleFuncSearchAdsByAdvertiserID(w http.ResponseWriter, req *http.Request) {
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

	// search all ads by advertiser id
	allAdsByAdvertiserID, err := selectAllAdsByAdvertiserID(ad.AdvertiserID)
	if err != nil {
		if err.Error() == "Failed to connect the database" {
			http.Error(w, "Failed to connect the database.", 500)
		} else if err.Error() == "Failed to select ads by advertiser_id from MySQL database" {
			http.Error(w, "Failed to select ads by advertiser_id from MySQL database.", 500)
		} else if err.Error() == "Failed to convert MySQL data into Ad type" {
			http.Error(w, "Failed to convert MySQL data into Ad type.", 500)
		}
		return
	}

	// convert chosen ad data into Json format
	allAdsByAdvertiserIDJSON, err := json.Marshal(allAdsByAdvertiserID)
	if err != nil {
		http.Error(w, "Failed to parse allAds into JSON format", 500)
		fmt.Printf("Failed to parse allAds into JSON format %v.\n", err)
		return
	}

	w.Write(allAdsByAdvertiserIDJSON)
}

func deleteAd(ad Ad) error {
	// connect to database
	db, err := sql.Open("mysql", mysqlDataSourceName)
	if err != nil {
		return errors.New("Failed to connect MySQL database")
	}
	defer db.Close()

	// delete ad
	del, err := db.Prepare("DELETE FROM ad WHERE ad_id=?")
	if err != nil {
		return errors.New("Failed to delete ad")
	}
	del.Exec(ad.AdID)
	defer del.Close()

	return nil

}

func handleFuncDeleteAd(w http.ResponseWriter, req *http.Request) {
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

	// delete the ad
	err := deleteAd(ad)
	if err != nil {
		if err.Error() == "Failed to connect the database" {
			http.Error(w, "Failed to connect the database.", 500)
		} else if err.Error() == "Failed to delete ad" {
			http.Error(w, "Failed to delete ad.", 500)
		}
	}

	w.Write([]byte("Successfully deleted an ad."))

}
