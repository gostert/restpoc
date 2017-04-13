// restpoc project main.go
package main

import (
	"database/sql"
	"encoding/json"
	//	"errors"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/Knetic/govaluate"
	_ "github.com/go-sql-driver/mysql" // _ means for side effect of initialization or registration
	"github.com/jinzhu/now"
)

var db *sql.DB
var txChannel = make(chan TxInfo, 10000)
var custCache = map[int64]Cust{}
var acctCache = map[int64]Acct{}
var credential map[string]string
var custAttMap = make(map[int64]map[string]interface{}) // customer column cache map
var acctAttMap = make(map[int64]map[string]interface{}) // Account column cache map
var mu = &sync.Mutex{}

var txTestCache = map[string]*TxTest{}
var txFilterCache = map[string]*TxFilter{}
var txDefFilters []string             // default filters
var txDefFilterGrp TxFilterGrp        // global default filter group
var paramColMap = map[string]ColMap{} // global allowed param to column maps

type ColMap struct {
	Param   string // test param, e.g. amtDebit
	CustCol string // column for customer based test
	AcctCol string // column for account based test
}

type Page struct {
	Title string
	Body  []byte
}

func main() {
	var err error
	db, err = sql.Open("mysql", "leedev:Sanmateo347@tcp(34.206.152.113:3306)/restapi")
	if err != nil {
		log.Panic(err)
	}
	db.SetMaxOpenConns(100)
	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(10)

	// sql.DB should be long lived "defer" closes it once this function ends
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Panic(err)
	}
	now.FirstDayMonday = true
	initParamColMaps()

	// prepare TxTests and Filters
	ok := initTests()
	if ok == -1 {
		log.Panic("Error Caching TxTests")
	}

	ok = initFilters()
	if ok == -1 {
		log.Panic("Error Caching TxTests")
	}

	// spin up 10 update "workers" for each machine, this will be paramterized in production
	for i := 0; i < 4; i++ {
		go updateTX()
	}

	/*
		_ = "breakpoint"
		p1 := &Page{Title: "GoPage", Body: []byte("This is a sample Page.")}
		p1.save()
		p2, _ := loadPage("GoPage")
		fmt.Println(string(p2.Body))
	*/
	//  timeFunctions()
	//	randomNumbers()
	// govaluateFunctions()

	initCredential()

	http.HandleFunc("/view/", LHFHandleFunc(viewHandler))
	http.HandleFunc("/edit/", LHFHandleFunc(editHandler))
	http.HandleFunc("/save/", LHFHandleFunc(saveHandler))
	http.HandleFunc("/json/", LHFHandleFunc(jsonHandler))
	http.HandleFunc("/getrules/", LHFHandleFunc(getrulesHandler))
	http.HandleFunc("/check/", LHFHandleFunc(checkHandler))
	http.HandleFunc("/addCust/", LHFHandleFunc(addCustHandler))
	http.HandleFunc("/getCust/", LHFHandleFunc(getCustHandler))
	http.HandleFunc("/getAllCust/", LHFHandleFunc(getAllCustHandler))
	http.HandleFunc("/getAcct/", LHFHandleFunc(getAcctHandler))
	http.HandleFunc("/processTxnoj/", LHFHandleFunc(processTxnojHandler))
	http.HandleFunc("/processTx/", LHFHandleFunc(processTxHandler))

	sPath := os.Getenv("GOPATH")

	err = http.ListenAndServeTLS("0.0.0.0:8080", sPath+"/src/github.com/gostert/rsa/leecert.pem", sPath+"/src/github.com/gostert/rsa/leekey.pem", nil)
	// err = http.ListenAndServe("0.0.0.0:8080", nil)
	log.Fatal(err)
}

func timeFunctions() float32 { // time related functions
	start := time.Now()
	fmt.Println("time.Now():", time.Now())
	//init the loc
	zone, _ := time.LoadLocation("America/Los_Angeles")
	Eastern := time.FixedZone("EDT", -5*3600)
	Pacific := time.FixedZone("PDT", -8*3600)

	now.FirstDayMonday = true
	t := time.Date(2017, 02, 18, 17, 51, 49, 123456789, time.Now().Location())

	fmt.Println("BeginingOfDay, wk, month, qtr, year", now.New(t).BeginningOfDay(), now.New(t).BeginningOfWeek(), now.New(t).BeginningOfMonth(), now.New(t).BeginningOfQuarter(), now.New(t).BeginningOfYear())
	fmt.Println("BeginingOfWeek, Month, qtr, year", now.BeginningOfWeek(), now.BeginningOfMonth(), now.BeginningOfQuarter(), now.BeginningOfYear())
	fmt.Println("EndOfWeek, Month, qtr, year", now.EndOfWeek(), now.EndOfMonth(), now.EndOfQuarter(), now.EndOfYear())

	//set timezone,

	fmt.Println("Time in zone, estern, pacific", time.Now().In(zone), time.Now().In(Eastern), time.Now().In(Pacific))
	fmt.Println("Time formate in estern, pacific", time.Now().In(Eastern).Format("01/02/2006 15:04:05 MST"), time.Now().In(Pacific).Format("01/02/2006 15:04:05 MST"))
	fmt.Printf("Time format:%s, %s\n", start.Format("01/02/2006 15:04:05 MST"), start.Format("01/02/2006 15:04:05 -07 MST"))

	timenano := time.Now().UnixNano()
	fmt.Println("time.Now() in nano seconds:", timenano)
	fmt.Println("time in string format back from nano:", time.Unix(0, timenano))
	return float32(time.Since(start)) / 1000
}

func randomNumbers() float32 { //function for generating random numbers
	start := time.Now()
	fmt.Println("*** start randomNumbers function")
	fmt.Println("rand.Intn(100)", rand.Intn(100))
	// Create and seed the generator.
	// Typically a non-fixed seed should be used, such as time.Now().UnixNano().
	// Using a fixed seed will produce the same output on every run.

	r := rand.New(rand.NewSource(start.UnixNano()))

	// The tabwriter here helps us generate aligned output.
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
	defer w.Flush()

	show := func(name string, v1, v2, v3 interface{}) {
		fmt.Fprintf(w, "%s\t%v\t%v\t%v\n", name, v1, v2, v3)
	}

	// Float32 and Float64 values are in [0, 1).
	show("Float32", r.Float32(), r.Float32(), r.Float32())
	show("Float64", r.Float64(), r.Float64(), r.Float64())

	// Int31, Int63, and Uint32 generate values of the given width.
	// The Int method (not shown) is like either Int31 or Int63
	// depending on the size of 'int'.
	show("Int31", r.Int31(), r.Int31(), r.Int31())
	show("Int63", r.Int63(), r.Int63(), r.Int63())
	show("Uint32", r.Uint32(), r.Uint32(), r.Uint32())

	// Intn, Int31n, and Int63n limit their output to be < n.
	// They do so more carefully than using r.Int()%n.
	show("Intn(10)", r.Intn(2), r.Intn(2), r.Intn(2))
	show("Int31n(10)", r.Int31n(10), r.Int31n(10), r.Int31n(10))
	show("Int63n(10)", r.Int63n(10), r.Int63n(10), r.Int63n(10))

	// Perm generates a random permutation of the numbers [0, n).
	show("Perm", r.Perm(5), r.Perm(5), r.Perm(5))

	return float32(time.Since(start)) / 1000
}

func govaluateFunctions() { // dynamic express evaluations
	expression, err := govaluate.NewEvaluableExpression("10 > 0")
	result, err := expression.Evaluate(nil)
	// result is now set to "true", the bool value.
	fmt.Println("10>0:", result, err)

	expression, err = govaluate.NewEvaluableExpression("(20<=t1 && t1<=40) && (t2>2)")

	parameters := make(map[string]interface{}, 8)
	parameters["t1"] = int64(35)
	parameters["t2"] = int64(3)

	result, err = expression.Evaluate(parameters)
	// result is now set to "false", the bool value.
	fmt.Println("with 2 parameters:", result, err)
}

func LHFHandleFunc(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tstart := time.Now()

		username, password, _ := r.BasicAuth()
		// fmt.Printf("\nBasic Auth user:%s, password:%s\n", username, password)

		if pass, ok := credential[username]; ok {
			if password != pass {
				http.Error(w, "Invalid Password", http.StatusUnauthorized)
				fmt.Fprintf(w, "\nTime Elapse:%v", time.Since(tstart))
				return
			}
		} else {
			http.Error(w, "Invalid Username", http.StatusUnauthorized)
			fmt.Fprintf(w, "\nTime Elapse:%v", time.Since(tstart))
			return
		}

		fn(w, r)
		fmt.Fprintf(w, "\nTime Elapse:%v", time.Since(tstart))
	}
}

func (p *Page) save() error {
	filename := p.Title + ".txt"
	return ioutil.WriteFile(filename, p.Body, 0600)
}

func loadPage(title string) (*Page, error) {
	filename := title + ".txt"
	body, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return &Page{Title: title, Body: body}, nil
}

func viewHandler(w http.ResponseWriter, r *http.Request) {
	title := r.URL.Path[len("/view/"):]
	p, _ := loadPage(title)
	t, _ := template.ParseFiles("viewpage_template.html")
	t.Execute(w, p)
}

func editHandler(w http.ResponseWriter, r *http.Request) {
	title := r.URL.Path[len("/edit/"):]
	p, err := loadPage(title)
	if err != nil {
		p = &Page{Title: title}
	}
	t, _ := template.ParseFiles("editpage_template.html")
	t.Execute(w, p)
}

func saveHandler(w http.ResponseWriter, r *http.Request) {
	title := r.URL.Path[len("/save/"):]
	body := r.FormValue("body")
	p := &Page{Title: title, Body: []byte(body)}
	p.save()
	http.Redirect(w, r, "/view/"+title, http.StatusFound)
}

func jsonHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/json/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != "POST" { // expecting POST method
		http.Error(w, "Invalid request method.", 405)
		return
	}

	decoder := json.NewDecoder(r.Body)
	var in TxInput

	err := decoder.Decode(&in)
	if err != nil {
		panic(err)
	}

	defer r.Body.Close()

	fmt.Fprintf(w, "rules: %+v", in)
}

func getrulesHandler(w http.ResponseWriter, r *http.Request) {

	if r.URL.Path != "/getrules/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != "GET" { // expecting get method
		http.Error(w, "Invalid request method.", 405)
		return
	}

	var rules string

	rid := r.URL.Query().Get("rid")
	if len(rid) == 0 {
		rules = "{name: 'dialyfrequency'; condition: '< 4'}"
	} else {
		rules = "{id:" + rid + "; name: 'dialyfrequency'; condition: '< 4'}"
	}
	fmt.Fprintf(w, "rules: %s", rules)
}

// test transaction process model function
// check "num" of functions each takes a randome amount of time smaller than "msec"
func checkHandler(w http.ResponseWriter, r *http.Request) {

	// expected url: http://localhost:8080/check/?num=10&msec=3

	if r.URL.Path != "/check/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != "GET" { // Use GET for simplicity, will use POST for real
		http.Error(w, "Invalid request method.", 405)
		return
	}

	// parse parameters
	num, _ := strconv.Atoi(r.URL.Query().Get("num"))
	msec, _ := strconv.Atoi(r.URL.Query().Get("msec"))

	// real verification function
	// print out the input, time spent in each function, and total time
	checkTest(num, msec, w)

}

func checkTest(num int, msec int, w http.ResponseWriter) { //nseq is the number of sends in a channel, npara is the number of channels
	ch := make([]chan string, num) // initialize channel slice
	for i := range ch {
		ch[i] = make(chan string, 1)
	}

	var wg sync.WaitGroup

	outn := make([]int, num) // holds the random number for each task
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	fmt.Fprintf(w, "input num = %d, nsec = %d \n", num, msec)

	for i, chans := range ch {
		outn[i] = r.Intn(msec)
		wg.Add(1)
		go func(cha chan string, ii int) {
			tstart := time.Now()
			defer wg.Done()
			time.Sleep(time.Duration(outn[ii]+1) * time.Second)
			cha <- "Task[" + strconv.Itoa(ii) + "](" + strconv.Itoa(outn[ii]) + ") took: " + time.Since(tstart).String() + "\n"
		}(chans, i)
	}

	//	time.Sleep(time.Duration(msec) * time.Second)

	wg.Wait()

	var outputstring string

	for i, _ := range outn {
		outputstring = outputstring + <-ch[i]
	}

	fmt.Fprintf(w, "Output:\n%s", outputstring)
}

func send(nseq int, c chan string, ncnt int) {
	time.Sleep(time.Duration(150*nseq) * time.Millisecond)
	c <- "channel:" + strconv.Itoa(ncnt)
}

func receive(c chan string, w http.ResponseWriter) {
	s := <-c
	fmt.Fprintf(w, "%s", s)
}

type Cust struct {
	CustID     int64
	FiID       int
	NName      string
	FName      string
	LName      string
	Dob        string
	Cur        string
	TotCDebit  int64
	TotCCredit int64
	Bal        int64
	TotFee     int
	CntCDebit  int
	CntCCredit int
	CntCTot    int
	Dup        int
}

type Acct struct {
	CustID       int64
	AcctID       int64
	Cur          string
	CurPrecision int
	Bal          int64
	TotDebit     int64
	TotCredit    int64
	TotFee       int
	CntDebit     int
	CntCredit    int
	CntTot       int
	Dup          int
}

// create sortable customer and account slices
type CustSlice []Cust
type AcctSlice []Acct

func (custs CustSlice) Len() int {
	return len(custs)
}

func (accts AcctSlice) Len() int {
	return len(accts)
}

func (custs CustSlice) Less(i, j int) bool {
	return custs[i].CustID < custs[j].CustID
}

func (accts AcctSlice) Less(i, j int) bool {
	if accts[i].CustID < accts[j].CustID {
		return true
	} else if accts[i].CustID == accts[j].CustID {
		return accts[i].AcctID < accts[j].AcctID
	} else {
		return false
	}
}

func (custs CustSlice) Swap(i, j int) {
	custs[i], custs[j] = custs[j], custs[i]
}

func (accts AcctSlice) Swap(i, j int) {
	accts[i], accts[j] = accts[j], accts[i]
}

// return a single customer record in the form of customer structure
func getCust(cid int64) (*Cust, error) {
	var cust Cust

	cust, ok := custCache[cid]

	if ok {
		return &cust, nil
	} else {
		fmt.Printf("\nQuery new customer: %d", cid)
		err := db.QueryRow("SELECT nName, totCDebit, totCCredit, bal, cntCDebit, cntCCredit, cntCTot,totFee FROM CUST where idCUST = ?", cid).Scan(
			&cust.NName, &cust.TotCDebit, &cust.TotCCredit, &cust.Bal, &cust.CntCDebit, &cust.CntCCredit, &cust.CntCTot, &cust.TotFee)

		switch {
		case err == sql.ErrNoRows:
			log.Fatal(err)
			return nil, err
		case err != nil:
			log.Fatal(err)
			return nil, err
		default:
			cust.CustID = cid
			custCache[cid] = cust
			custAttMap[cid] = make(map[string]interface{})
			custAttMap[cid]["totCDebit"] = cust.TotCDebit
			custAttMap[cid]["totCCredit"] = cust.TotCCredit
			custAttMap[cid]["totCFee"] = int64(cust.TotFee)
			custAttMap[cid]["cntCTot"] = int64(cust.CntCTot)
			custAttMap[cid]["cntCDebit"] = int64(cust.CntCDebit)
			custAttMap[cid]["cntCCredit"] = int64(cust.CntCCredit)
			return &cust, nil
		}
	}

}

// return a single customer record in the form of customer structure
func getAcct(accID int64) (*Acct, error) {
	var acct Acct

	acct, ok := acctCache[accID]

	if ok {
		return &acct, nil
	} else {

		err := db.QueryRow("SELECT idCUST, cur, curPrecision, bal, totDebit, totCredit, totFee, cntDebit, cntCredit, cntTot FROM ACCT where idACCT = ?", accID).Scan(
			&acct.CustID, &acct.Cur, &acct.CurPrecision, &acct.Bal, &acct.TotDebit, &acct.TotCredit, &acct.TotFee, &acct.CntDebit, &acct.CntCredit, &acct.CntTot)

		switch {
		case err == sql.ErrNoRows:
			log.Fatal(err)
			return nil, err
		case err != nil:
			log.Fatal(err)
			return nil, err
		default:
			acct.AcctID = accID
			acctCache[accID] = acct
			acctAttMap[accID] = make(map[string]interface{})
			acctAttMap[accID]["totDebit"] = acct.TotDebit
			acctAttMap[accID]["totCredit"] = acct.TotCredit
			acctAttMap[accID]["totFee"] = int64(acct.TotFee)
			acctAttMap[accID]["cntTot"] = int64(acct.CntTot)
			acctAttMap[accID]["cntDebit"] = int64(acct.CntDebit)
			acctAttMap[accID]["cntCredit"] = int64(acct.CntCredit)
			return &acct, nil
		}
	}

}

// get customer handler
func getCustHandler(w http.ResponseWriter, r *http.Request) {

	// experected url: /getCust/?cid=83837383748
	if r.URL.Path != "/getCust/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != "GET" { // expecting GET method
		http.Error(w, "Invalid request method.", 405)
		return
	}

	var cid int64

	// parse parameters, cid is the customer id
	fmt.Printf("input:%v\n", r.URL.Query().Get("cid"))
	cid, _ = strconv.ParseInt(r.URL.Query().Get("cid"), 10, 64)

	fmt.Fprintf(w, "\nget customer:%d\n", cid)

	cust, err := getCust(cid)
	if err != nil {
		fmt.Fprintf(w, "%v", err)
	} else {
		fmt.Fprintf(w, "%d, %+v", cust.CustID, *cust)
	}

}

// get Account handler
func getAcctHandler(w http.ResponseWriter, r *http.Request) {

	// experected url: /getAcct/?acctid=83837383748
	if r.URL.Path != "/getAcct/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != "GET" { // expecting GET method
		http.Error(w, "Invalid request method.", 405)
		return
	}

	var cid int64

	// parse parameters, cid is the customer id
	acctid, _ := strconv.ParseInt(r.URL.Query().Get("acctid"), 10, 64)

	fmt.Fprintf(w, "\nget account:%d\n", cid)

	acct, err := getAcct(acctid)
	if err != nil {
		fmt.Fprintf(w, "%v", err)
	} else {
		fmt.Fprintf(w, "%d, %+v", acct.AcctID, *acct)
	}

}

// Init user and password
func initCredential() error {
	credential = map[string]string{
		"leedev": "Go123",
		"guest":  "xxx"}

	return nil
}

// return a single customer record in the form of customer structure
func getAllCust() ([]*Cust, error) {

	rows, err := db.Query("SELECT idCUST, nName, totCDebit, totCCredit, bal, cntCDebit, cntCCredit, cntCTot FROM CUST")

	if err != nil {
		log.Fatal(err)
	}

	// defer rows.Close()
	custs := make([]*Cust, 0)
	for rows.Next() {
		cust := new(Cust)
		err := rows.Scan(&cust.CustID, &cust.NName, &cust.TotCDebit, &cust.TotCCredit, &cust.Bal, &cust.CntCDebit, &cust.CntCCredit, &cust.CntCTot)
		if err != nil {
			rows.Close()
			log.Fatal(err)
		}
		custs = append(custs, cust)
	}
	rows.Close()
	return custs, err
}

// get all customer handler
func getAllCustHandler(w http.ResponseWriter, r *http.Request) {

	// experected url: /getAllCust/
	if r.URL.Path != "/getAllCust/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != "GET" { // expecting GET method
		http.Error(w, "Invalid request method.", 405)
		return
	}

	fmt.Fprintf(w, "\nget all customer:\n")

	custs, err := getAllCust()
	if err != nil {
		fmt.Fprintf(w, "%v", err)
	} else {
		for _, cust := range custs {
			fmt.Fprintf(w, "%+v", *cust)
		}
	}

}

// add customer handler
func addCustHandler(w http.ResponseWriter, r *http.Request) {

	// experected url: /addCust/?num=3$pcnt=1
	if r.URL.Path != "/addCust/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != "GET" { // expecting GET method
		http.Error(w, "Invalid request method.", 405)
		return
	}

	// parse parameters, num is the number of insert, pcnt if the parallet task count
	num, _ := strconv.Atoi(r.URL.Query().Get("num"))
	pcnt, _ := strconv.Atoi(r.URL.Query().Get("pcnt"))

	ch := make([]chan string, pcnt) // initialize channel slice
	for i := range ch {
		ch[i] = make(chan string, 1)
	}

	var wg sync.WaitGroup

	fmt.Fprintf(w, "row per channel num = %d, number of channels = %d \n", num, pcnt)

	for i, chans := range ch {
		wg.Add(1)
		go func(cha chan string, ii int) {
			tstart := time.Now()
			defer wg.Done()
			addCust(num)
			cha <- "Channel[" + strconv.Itoa(ii) + "] took: " + time.Since(tstart).String() + "\n"
		}(chans, i)
	}

	wg.Wait()

	var outputstring string

	for i := 0; i < pcnt; i++ {
		outputstring = outputstring + <-ch[i]
	}

	fmt.Fprintf(w, "Output:\n%s", outputstring)
}

// add cnt number of customer to the database with random name and data
// add 1-5 accounts for each customer with random data
func addCust(cnt int) sql.Result {

	var FiCIDpadding, FiAIDpadding, CidMax, AidMax int64
	FiCIDpadding = 100100000000000  // 4 digit FI id, 11 digit customer id
	FiAIDpadding = 1001000000000000 // 4 digit FI id, 12 digit account id
	FiID := 1001
	CidMax = 99999999999
	AidMax = 999999999999
	fNameArr := []string{"Mike", "Dale", "Jen", "Bob", "Judi", "Susan", "Lee", "Rachel", "Jenny"}
	state := []string{"PA", "DE", "CA", "NY", "TX", "NJ", "FL"}
	r := rand.New(rand.NewSource(time.Now().UnixNano())) // random seed

	sqlCust := "INSERT INTO CUST (idCUST, idFI, nName, fName, state, country, last4SSN, cur, totCDebit, totCCredit, bal, cntCDebit, cntCCredit, cntCTot,totFee) VALUES "
	sqlAcct := "INSERT INTO ACCT (idCUST, idACCT, cur, curPrecision, bal) VALUES "

	accRange := 5 // 1 to 5 accounts per customer

	var fName, cIDs string
	var bal, balC, nAcct, baltot int

	for i := 0; i < cnt; i++ {
		fName = fNameArr[r.Intn(len(fNameArr))] // random first name
		cIDs = strconv.FormatInt((FiCIDpadding + r.Int63n(CidMax) + 1), 10)
		balC = r.Intn(100000)
		sqlCust += "(" + cIDs + ", " + strconv.Itoa(FiID) +
			", '" + fName + strconv.Itoa(r.Intn(9998)+1) + "','" + fName + "', '" + state[r.Intn(len(state))] +
			"', 'USA', '" + strconv.Itoa(r.Intn(9999)) + "', 'USA', 0, 0," + strconv.Itoa(balC) + ", 0, 0, 0, 0), "
		nAcct = r.Intn(accRange)
		baltot = 0
		for j := 0; j <= nAcct; j++ { // assign random balances htat add up to cust balance total
			if nAcct == 0 {
				bal = balC
			} else if j == nAcct {
				bal = balC - baltot
			} else {
				bal = r.Intn(balC / nAcct)
				baltot += bal
			}
			sqlAcct += "(" + cIDs + ", " + strconv.FormatInt((FiAIDpadding+r.Int63n(AidMax)+1), 10) +
				", 'USD', 2, " + strconv.Itoa(bal) + "), "
		}
	}

	var res sql.Result

	//trim the last ,
	sqlCust = sqlCust[0:len(sqlCust)-2] + " on duplicate key update lname='dup';"
	sqlAcct = sqlAcct[0:len(sqlAcct)-2] + " on duplicate key update bal=499;"

	res, err := db.Exec(sqlCust)
	if err != nil {
		panic("\nCustomer Insert Statement error\n" + err.Error()) // proper error handling instead of panic in your app
	}

	res, err = db.Exec(sqlAcct)
	if err != nil {
		panic("\nAccout Insert Statement error\n" + err.Error()) // proper error handling instead of panic in your app
	}

	// fmt.Printf("\nCust Insert:\n%s\nAccount Insert:\n%s", sqlCust, sqlAcct)

	return res
}

type TxRec struct {
	CustID     int64
	TxID       int64
	HostTxID   string
	AcctID     int64
	FiID       int
	TxDateTime string
	Cur        string
	AmtDebit   int
	AmtFee     int
	TotDebit   int64
	AmtCredit  int
	TotCredit  int64
	TotFee     int
	Bal        int64
	TxCode     int
	CntDebit   int
	CntCredit  int
	CntTot     int
	TotCDebit  int64
	TotCCredit int64
	TotCFee    int
	BalCust    int64
	CntCDebit  int
	CntCCredit int
	CntCTot    int
	HostTxInfo string
	VerifyFlag int
	sumScore   string
	Safes      int
	Fails      int
	Dangers    int
	Maybes     int
	TxVersion  int
}

type TxInput struct {
	CustID     int64
	TxID       int64
	HostTxID   string
	AcctID     int64
	FiID       int
	TxDateTime string
	Cur        string
	AmtDebit   int
	AmtCredit  int
	AmtFee     int
	TxCode     int
	HostTxInfo string
	VerifyFlag int
}

type Period struct {
	Value      string // user input value
	IsRunning  int    // is the period running or from the begining of some time, 1 yes, 0. no
	PeriodType string // valid values: cur(current time), hr, day, wk, mth, qtr, yr
	Multiple   int    // defined period multiple
}

func (p *Period) getBegineTimeNano(t time.Time) int64 {
	if p.IsRunning == 1 {
		switch p.PeriodType {
		case "cur":
			return t.UnixNano()
		case "hr":
			return t.Add(-time.Duration(p.Multiple) * time.Hour).UnixNano()
		case "day":
			return t.AddDate(0, 0, -1*p.Multiple).UnixNano()
		case "wk":
			return t.AddDate(0, 0, -7*p.Multiple).UnixNano()
		case "mth":
			return t.AddDate(0, -1*p.Multiple, 0).UnixNano()
		case "qtr":
			return t.AddDate(0, -3*p.Multiple, 0).UnixNano()
		case "yr":
			return t.AddDate(-1*p.Multiple, 0, 0).UnixNano()
		default:
			panic("wrong periodType")
		}
	} else {
		switch p.PeriodType {
		case "hr":
			return now.New(t).BeginningOfHour().Add(-time.Duration(p.Multiple) * time.Hour).UnixNano()
		case "day":
			return now.New(t).BeginningOfDay().AddDate(0, 0, -1*p.Multiple).UnixNano()
		case "wk":
			return now.New(t).BeginningOfWeek().AddDate(0, 0, -7*p.Multiple).UnixNano()
		case "mth":
			return now.New(t).BeginningOfMonth().AddDate(0, -1*p.Multiple, 0).UnixNano()
		case "qtr":
			return now.New(t).BeginningOfQuarter().AddDate(0, -3*p.Multiple, 0).UnixNano()
		case "yr":
			return now.New(t).BeginningOfQuarter().AddDate(-1*p.Multiple, 0, 0).UnixNano()
		default:
			panic("wrong periodType")
		}
	}
}

type ColParam struct {
	Param     string // original param string
	Scalar    string // scalar function, empty if the no functions
	CName     string // column name
	TCName    string // transaction structure column(attribute) name
	QueryType int    // 1: no SQL, 4: sql test, avg test: 5, 9: aggregate test
	SqlColStr string // actual SQL column string
}

// TxTest is immutable
type TxTest struct {
	TName       string
	CategoryStr string
	Conditions  string
	ParamStr    string
	Params      []ColParam
	PeriodStr   string
	Period      Period
	ScoreMapStr string
	ScoreMap    map[string]string //e.g. expression string map for safe, fail, danger, maybe
	CustOrAcct  string
	QueryType   int // higher level of query type in the test
}

type TestGrp struct {
	SQLStr          string                   // SQL Select filds for the group.
	EmptyRowStr     string                   // SQL group empty SQL row string
	SQLPeriodIdx    map[string]*PeriodIdx    // SQL period index for a particular period, keyed by peroid string
	SQLColIdx       int                      // latest index of the SQL column
	SQLColPeriodIdx int                      // latest index of the SQL column period
	GrpParamIdx     map[string]int           // map of parameter order (index) on the SQL statement keyed by parameter string
	Tests           map[string]int           // map of active test, keyed by tested name. Int value can be used in the future
	TestParamValue  map[string][]interface{} // map of column value array corresonding to []Colparam parameter in the test, keyed by test
	TotalTest       int
}

type PeriodIdx struct {
	Period Period
	Idx    int
}

type TxFilter struct {
	FName      string
	FIID       int
	Conditions string
	CustTests  []string
	AcctTests  []string
	IsDef      int // is default filter flag
}

// combined filters built at runtime, taken in consideration of conditions
type TxFilterGrp struct {
	Filters       string  // comma separated original filter definition list
	ActiveFilters string  //
	CustTestGrp   TestGrp // all customer the test that required aggregation sorted by peroid in decending order
	AcctTestGrp   TestGrp // same for accout
}

type TestRes struct {
	TName         string `json:"tName"`
	Categories    string `json:"categories"`
	ParamStr      string `json:"params"`
	ParamValues   string `json:"paramValues"`
	Score         string `json:"score"`
	ConditionPass int    `json:"-"` // 0: not processed, 1: passed, 2: failed, to be processed
}

type TxResult struct {
	HostTxID       string    `json:"hostTxID,omitempty"`
	LHfTxID        int64     `json:"lhfTxID,omitempty"`
	CustID         int64     `json:"custID,omitempty"`
	AcctID         int64     `json:"acctID,omitempty"`
	TxDateTime     string    `json:"txDateTime,omitempty"`
	VerifyFlag     int       `json:"verifyFlag,omitempty"`
	Note           string    `json:"note,omitempty"`
	DefinedFilters string    `json:"-"`
	ActiveFilters  string    `json:"filters,omitempty"`
	TestCount      int       `json:"testCount,omitempty"`
	SumScore       string    `json:"sumScore,omitempty"`
	Safes          int       `json:"safes,omitempty"`
	Fails          int       `json:"fails,omitempty"`
	Dangers        int       `json:"dangers,omitempty"`
	Maybes         int       `json:"maybes,omitempty"`
	TestResults    []TestRes `json:"TestResults,omitempty"`
	ReturnStr      string    `json:"-"`
}

type TxInfo struct {
	TxIn    TxInput
	TestRes TxResult
	TxCust  Cust
	TxAcct  Acct
}

// Function that prepares and caches TxFilters, returns 0 if there are errors, success return total number of filters initialized
func initFilters() int {
	rows, err := db.Query("SELECT idFILTER, idFI, conditions, tests, isDefault FROM TXFILTER")

	if err != nil {
		log.Fatal(err)
	}

	// defer rows.Close()

	i := 0
	var teststr *string

	for rows.Next() {
		fil := new(TxFilter)
		err := rows.Scan(&fil.FName, &fil.FIID, &fil.Conditions, &teststr, &fil.IsDef)
		if err != nil {
			rows.Close()
			log.Fatal(err)
		}

		tststr := strings.Split(*teststr, ",") // parse categories
		fil.CustTests = make([]string, 0)
		fil.AcctTests = make([]string, 0)

		for _, test := range tststr {
			if txTestCache[test].CustOrAcct == "c" {
				fil.CustTests = append(fil.CustTests, strings.TrimSpace(test))
			} else if txTestCache[test].CustOrAcct == "a" {
				fil.AcctTests = append(fil.AcctTests, strings.TrimSpace(test))
			} else {
				panic("Error Parsing Filter on tests: " + txTestCache[test].CustOrAcct + test)
			}
		}

		txFilterCache[fil.FName] = fil
		if fil.IsDef == 1 {
			txDefFilters = append(txDefFilters, fil.FName)
		}
		i++
	}

	rows.Close()
	// build global default filter
	txDefFilterGrp = *new(TxFilterGrp)

	txDefFilterGrp.CustTestGrp.Tests = make(map[string]int)
	txDefFilterGrp.CustTestGrp.GrpParamIdx = make(map[string]int)
	txDefFilterGrp.CustTestGrp.TestParamValue = make(map[string][]interface{})
	txDefFilterGrp.CustTestGrp.SQLPeriodIdx = make(map[string]*PeriodIdx)

	txDefFilterGrp.AcctTestGrp.Tests = make(map[string]int)
	txDefFilterGrp.AcctTestGrp.GrpParamIdx = make(map[string]int)
	txDefFilterGrp.AcctTestGrp.TestParamValue = make(map[string][]interface{})
	txDefFilterGrp.AcctTestGrp.SQLPeriodIdx = make(map[string]*PeriodIdx)

	for _, filstr := range txDefFilters {
		if len(txDefFilterGrp.Filters) == 0 {
			txDefFilterGrp.Filters += txFilterCache[filstr].FName
		} else {
			txDefFilterGrp.Filters += "," + txFilterCache[filstr].FName
		}
		if len(txDefFilterGrp.ActiveFilters) == 0 {
			txDefFilterGrp.ActiveFilters += txFilterCache[filstr].FName
		} else {
			txDefFilterGrp.ActiveFilters += "," + txFilterCache[filstr].FName
		}

		buildTestGrp(&txDefFilterGrp.CustTestGrp, txFilterCache[filstr].CustTests)
		buildTestGrp(&txDefFilterGrp.AcctTestGrp, txFilterCache[filstr].AcctTests)

	}

	fmt.Printf("\nDefault Filters:%+v", txDefFilters)
	fmt.Printf("\nDefault Filter group:%+v", txDefFilterGrp)

	//	return custs, err
	return i
}

// Build test group from list of tests
func buildTestGrp(tBaseGrp *TestGrp, addTests []string) error {
	avgColStr := ""
	// add to SQLtest group from the filter tests one at a time
	for _, tst := range addTests {
		if _, ok := tBaseGrp.Tests[tst]; !ok {
			tBaseGrp.Tests[tst] = 0 // register exist. Actual value can be used in the future
			tBaseGrp.TotalTest++
			tBaseGrp.TestParamValue[tst] = make([]interface{}, len(txTestCache[tst].Params)) // initialize the value array
			for _, para := range txTestCache[tst].Params {                                   // going through each parameter in a test
				if para.QueryType == 4 || para.QueryType == 5 { // only process the logic for SQL types of the parameter
					// iCol := tBaseGrp.SQLColIdx // Current group column index
					if len(tBaseGrp.SQLStr) == 0 {
						if para.QueryType == 5 { // process for AVG type parameter
							if txTestCache[tst].CustOrAcct == "c" {
								avgColStr = paramColMap["CNT("+para.CName+")"].CustCol
							} else {
								avgColStr = paramColMap["CNT("+para.CName+")"].AcctCol
							}
							tBaseGrp.SQLStr += para.SqlColStr + "," + avgColStr
							tBaseGrp.EmptyRowStr += "0,0"
							tBaseGrp.GrpParamIdx[para.SqlColStr] = tBaseGrp.SQLColIdx
							tBaseGrp.SQLColIdx++
							tBaseGrp.GrpParamIdx[avgColStr] = tBaseGrp.SQLColIdx
							tBaseGrp.SQLColIdx++
						} else {
							tBaseGrp.SQLStr += para.SqlColStr
							tBaseGrp.EmptyRowStr += "0"
							tBaseGrp.GrpParamIdx[para.SqlColStr] = tBaseGrp.SQLColIdx
							tBaseGrp.SQLColIdx++
						}
					} else {
						if _, ok := tBaseGrp.GrpParamIdx[para.SqlColStr]; !ok { // column doesn't exist
							if para.QueryType == 5 { // process for AVG type parameter
								if txTestCache[tst].CustOrAcct == "c" {
									avgColStr = paramColMap["CNT("+para.CName+")"].CustCol
								} else {
									avgColStr = paramColMap["CNT("+para.CName+")"].AcctCol
								}
								if _, ok := tBaseGrp.GrpParamIdx[avgColStr]; !ok { // if the avg cnt column doesn't exist
									tBaseGrp.SQLStr += "," + para.SqlColStr + "," + avgColStr
									tBaseGrp.EmptyRowStr += ",0,0"
									tBaseGrp.GrpParamIdx[para.SqlColStr] = tBaseGrp.SQLColIdx
									tBaseGrp.SQLColIdx++
									tBaseGrp.GrpParamIdx[avgColStr] = tBaseGrp.SQLColIdx
									tBaseGrp.SQLColIdx++
								} else {
									tBaseGrp.SQLStr += "," + para.SqlColStr
									tBaseGrp.EmptyRowStr += ",0"
									tBaseGrp.GrpParamIdx[para.SqlColStr] = tBaseGrp.SQLColIdx
									tBaseGrp.SQLColIdx++
								}
							} else {
								tBaseGrp.SQLStr += "," + para.SqlColStr
								tBaseGrp.EmptyRowStr += ",0"
								tBaseGrp.GrpParamIdx[para.SqlColStr] = tBaseGrp.SQLColIdx
								tBaseGrp.SQLColIdx++
							}
						}
					}

					if para.QueryType == 4 || para.QueryType == 5 { // only count period for querytype 4 or 5
						if _, ok := tBaseGrp.SQLPeriodIdx[txTestCache[tst].PeriodStr]; !ok { // column period index
							tBaseGrp.SQLPeriodIdx[txTestCache[tst].PeriodStr] = &PeriodIdx{txTestCache[tst].Period, -1}
							tBaseGrp.SQLColPeriodIdx++
						}
					}
				}
			}
		}
	}
	return nil
}

// initialized param to column maps
func initParamColMaps() int {
	paramColMap["SUM(amtDebit)"] = ColMap{"SUM(amtDebit)", "totCDebit", "totDebit"}
	paramColMap["SUM(amtCredit)"] = ColMap{"SUM(amtCredit)", "totCCredit", "totCredit"}
	paramColMap["SUM(amtFee)"] = ColMap{"SUM(amtFee)", "totCFee", "totFee"}
	paramColMap["CNT(*)"] = ColMap{"COUNT(*)", "cntCTot", "cntTot"}
	paramColMap["CNT(amtDebit)"] = ColMap{"COUNT(*)", "cntCDebit", "cntDebit"}
	paramColMap["CNT(amtCredit)"] = ColMap{"COUNT(*)", "cntCCredit", "cntCredit"}
	paramColMap["AVG(amtDebit)"] = ColMap{"AVG(amtDebit)", "totCDebit", "totDebit"}
	paramColMap["AVG(amtCredit)"] = ColMap{"AVG(amtCredit)", "totCCredit", "totCredit"}
	return 1
}

// Function that prepares and caches TxTests, returns 0 if there are errors, success return total number of tests initialized
func initTests() int {
	rows, err := db.Query("SELECT idTEST, categories, conditions, params, period, scoreMap, custORacct FROM TXTEST")

	if err != nil {
		log.Fatal(err)
	}
	// defer rows.Close()
	i := 0
	for rows.Next() {
		test := new(TxTest)
		err := rows.Scan(&test.TName, &test.CategoryStr, &test.Conditions, &test.ParamStr, &test.PeriodStr, &test.ScoreMapStr, &test.CustOrAcct)
		if err != nil {
			rows.Close()
			log.Fatal(err)
		}
		test.Params, test.QueryType = parseParams(strings.Split(test.ParamStr, ","), test.CustOrAcct)
		test.Period = *parsePeriod(test.PeriodStr)
		test.ScoreMap = parseScoreMap(test.ScoreMapStr)

		txTestCache[test.TName] = test
		i++
		fmt.Printf("\ntest %s: %+v", txTestCache[test.TName].TName, txTestCache[test.TName])
	}
	rows.Close()
	//	return custs, err
	return i
}

func parseScoreMap(scoreMapStr string) map[string]string {

	scoreMap := make(map[string]string)
	s := strings.Split(scoreMapStr, ";")
	var scorestr []string
	for _, str := range s {
		scorestr = strings.Split(strings.TrimSpace(str), ":")
		scoreMap[scorestr[0]] = strings.TrimSpace(scorestr[1])
	}
	return scoreMap
}

func parsePeriod(periodStr string) *Period {

	per := new(Period)

	per.Value = periodStr
	s := strings.Split(periodStr, "-")
	if len(s) == 1 {
		per.IsRunning = 1
		per.PeriodType = "cur"
		per.Multiple = 0
	} else {
		per.PeriodType = s[1]
		if s[0][0] == '0' {
			per.IsRunning = 0
			if len(s[0]) == 1 {
				per.Multiple = 0
			} else {
				per.Multiple, _ = strconv.Atoi(s[0][1:])
			}
		} else {
			per.IsRunning = 1
			per.Multiple, _ = strconv.Atoi(s[0])
		}
	}
	return per
}

func parseParams(paramStr []string, cORa string) ([]ColParam, int) {
	colParams := make([]ColParam, 0)
	param := new(ColParam)

	var strarr []string
	var re *regexp.Regexp
	qtype := 0 // 0: No test, 1; nosql test, 4: sql 5: avg, test, 9: aggregate test

	// fmt.Printf("\ncolmap in para:%+v", paramColMap)
	for _, str := range paramStr {
		re = regexp.MustCompile("(\\w{3})\\((.*?)\\)") // expecting scalar functions all have 3 letters
		strarr = re.FindStringSubmatch(strings.TrimSpace(str))

		if len(strarr) == 3 { // 3 letter scalar functino
			param.Param = strarr[0]
			param.Scalar = strarr[1]
			param.CName = strarr[2]
			param.TCName = strings.Title(strarr[2])
			// fmt.Printf("\nparsed para:%+v", param)
			if param.Scalar == "CNT" || param.Scalar == "SUM" || param.Scalar == "AVG" {
				if cORa == "c" {
					param.SqlColStr = paramColMap[strarr[0]].CustCol
				} else {
					param.SqlColStr = paramColMap[strarr[0]].AcctCol
				}
				if param.Scalar == "AVG" {
					param.QueryType = 5
				} else {
					param.QueryType = 4
				}
			} else { // MIN,MAX,STD,VAR, AVG
				param.SqlColStr = strarr[0]
				param.QueryType = 9
			}
		} else { // no SQL query for customer (c), account(a), or transaction (t)
			s := strings.Split(strings.TrimSpace(str), ".")
			// fmt.Printf("\nparse string: %s,%v", s)
			param.Param = strings.TrimSpace(str)
			param.Scalar = s[0]
			param.CName = s[1]
			param.TCName = s[1] // upper case the first letter
			param.QueryType = 1
			param.SqlColStr = ""
		}

		if param.QueryType > qtype {
			qtype = param.QueryType
		}
		colParams = append(colParams, *param)
	}

	return colParams, qtype
}

// add customer handler
func processTxnojHandler(w http.ResponseWriter, r *http.Request) {

	// experected url: /processTxnoj/ (with form parameters:
	if r.URL.Path != "/processTxnoj/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != "POST" { // expecting POST method
		http.Error(w, "Invalid request method.", 405)
		return
	}

	// parse parameters
	cid, _ := strconv.Atoi(r.URL.Query().Get("cid"))
	acctid, _ := strconv.Atoi(r.URL.Query().Get("acctid"))
	AmtC, _ := strconv.ParseFloat(r.URL.Query().Get("camt"), 64)
	AmtD, _ := strconv.ParseFloat(r.URL.Query().Get("damt"), 64)
	fee, _ := strconv.ParseFloat(r.URL.Query().Get("fee"), 64)
	vFlag, _ := strconv.Atoi(r.URL.Query().Get("verifyflag"))
	hosttxid := r.URL.Query().Get("hosttxid")

	txIn := TxInput{
		CustID:     int64(cid),
		AcctID:     int64(acctid),
		AmtCredit:  int(AmtC * 100),
		AmtDebit:   int(AmtD * 100),
		VerifyFlag: vFlag,
		AmtFee:     int(fee * 100),
		HostTxID:   hosttxid,
	}

	// fmt.Printf("\nTX input:\n%+v\n", txIn)

	txResultStr := processTx(&txIn)

	fmt.Fprintf(w, "%s", txResultStr)
}

// Process transaction verifications, expecting json post input
func processTxHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/processTx/" {
		http.NotFound(w, r)
		return
	}

	if r.Method != "POST" { // expecting POST method
		http.Error(w, "Invalid request method.", 405)
		return
	}

	decoder := json.NewDecoder(r.Body)
	var txIn TxInput

	err := decoder.Decode(&txIn)
	if err != nil {
		panic(err)
	}

	defer r.Body.Close()

	// fmt.Printf("\nTX input:\n%+v\n", txIn)

	txResultStr := processTx(&txIn)

	fmt.Fprintf(w, "%s", txResultStr)
}

// Entry function for verify a single transaction
// Process verificationa and return results. Update any affected cache objects,
// Do not perform updates which is done by concurrent go routines
func processTx(txInput *TxInput) string {
	// generate txID and return header

	zone, _ := time.LoadLocation("America/New_York")
	t := time.Now().In(zone)
	txInput.TxID = t.UnixNano()
	txInput.TxDateTime = t.Format("2006-01-02 15:04:05 MST")

	rTxResult := TxResult{
		HostTxID:   txInput.HostTxID,
		LHfTxID:    txInput.TxID,
		CustID:     txInput.CustID,
		AcctID:     txInput.AcctID,
		TxDateTime: txInput.TxDateTime,
	}

	// get customer and account object
	mu.Lock() // lock the global cust and acct maps
	cust, err := getCust(txInput.CustID)
	if err != nil {
		fmt.Printf("Error: %s", err)
		mu.Unlock()
		return err.Error()
	}
	// fmt.Printf("\nGet Cust:%+v\n", cust)
	acct, err := getAcct(txInput.AcctID)
	if err != nil {
		fmt.Printf("Error: %s", err)
		mu.Unlock()
		return err.Error()
	}
	// fmt.Printf("\nGet Acct:%+v\n", acct)

	rTxResult.VerifyFlag = txInput.VerifyFlag
	if txInput.VerifyFlag == 0 { // normal verification request
		rTxResult.Note = "Verification performed, updates issued"

		rTxResult.TestResults = []TestRes{}

		// Prepare TX filter group
		txFilterGrp := txDefFilterGrp

		// upadte filter info on results
		rTxResult.DefinedFilters = txFilterGrp.Filters
		rTxResult.ActiveFilters = txFilterGrp.ActiveFilters

		// fmt.Printf("\nFilter group:%+v", txFilterGrp)

		// execute goroutines in parallel on all the groups
		// each group returns their test results

		tResCustGrp, tResAcctGrp := make([]TestRes, 0), make([]TestRes, 0)
		// tResAcctGrp := make([]TestRes, 0)

		var wg sync.WaitGroup

		wg.Add(1)
		go func(tRes *[]TestRes) {
			tt := time.Now()
			*tRes = append(*tRes, processCustTests(&txFilterGrp.CustTestGrp, rTxResult.CustID, rTxResult.AcctID, t, txInput)...)
			fmt.Printf("\ntime ResCustTest:%+v", time.Since(tt))
			wg.Done()
		}(&tResCustGrp)

		wg.Add(1)
		go func(tRes *[]TestRes) {
			tt := time.Now()
			*tRes = append(*tRes, processAcctTests(&txFilterGrp.AcctTestGrp, rTxResult.CustID, rTxResult.AcctID, t, txInput)...)
			fmt.Printf("\ntime ResAccTests:%+v", time.Since(tt))
			wg.Done()
		}(&tResAcctGrp)

		wg.Wait()

		// aggregate all the test results to a signle TxResult structure
		rTxResult.TestResults = append(rTxResult.TestResults, tResCustGrp...)
		rTxResult.TestResults = append(rTxResult.TestResults, tResAcctGrp...)

		rTxResult.TestCount = len(rTxResult.TestResults)

		for _, tTst := range rTxResult.TestResults {
			switch tTst.Score {
			case "s", "e": // safe
				rTxResult.Safes++
			case "d": // danger
				rTxResult.Dangers++
			case "m": // may be
				rTxResult.Maybes++
			case "f": // fail
				rTxResult.Fails++
			default:
				// dont' expect exceptions
				panic("\nUnexpected query type")
			}
		}

		if rTxResult.Fails > 0 {
			rTxResult.SumScore = "f"
		} else if rTxResult.Dangers > 0 {
			rTxResult.SumScore = "d"
		} else if rTxResult.Maybes > 0 {
			rTxResult.SumScore = "m"
		} else {
			rTxResult.SumScore = "s"
		}

	} else if txInput.VerifyFlag == 1 { // for verification correction, transaction update only
		rTxResult.SumScore = "n"
		rTxResult.Note = "Verification skipped, correction issued"

	} else { // transaction correction request with all other values, skip verification
		// set default return values only
		rTxResult.Note = "Verification skipped, updates issued"
		rTxResult.SumScore = "n"
	}

	// retStr, err := json.Marshal(rTxResult)
	retStr, err := json.MarshalIndent(rTxResult, "", "  ")

	if err != nil {
		fmt.Printf("Error: %s", err)
		return err.Error()
	}

	rTxResult.ReturnStr = string(retStr)

	go func() {

		// Future considerations:
		// 1. Transfer between same customer accounts, where cust.cnt + 1, but each account has a debit and credit entry,
		//   generate a suppliment entry in TX table for the cp accout with supplimental flag, same host TX flag
		// 2. Transfer between different customer, but same FI, add another customer entry count
		//   and suppliemental entry for the other account just like same customer transfer, with supplimental 2 flag

		if txInput.AmtDebit > 0 {
			acct.TotDebit += int64(txInput.AmtDebit)
			cust.TotCDebit += int64(txInput.AmtDebit)
			acct.Bal -= int64(txInput.AmtDebit)
			cust.Bal -= int64(txInput.AmtDebit)
			acct.CntDebit++
			cust.CntCDebit++
		}

		if txInput.AmtCredit > 0 {
			acct.TotCredit += int64(txInput.AmtCredit)
			cust.TotCCredit += int64(txInput.AmtCredit)
			acct.Bal += int64(txInput.AmtCredit)
			cust.Bal += int64(txInput.AmtCredit)
			acct.CntCredit++
			cust.CntCCredit++
		}

		if txInput.AmtFee > 0 {
			acct.Bal -= int64(txInput.AmtFee)
			cust.Bal -= int64(txInput.AmtFee)
			acct.TotFee += txInput.AmtFee
			cust.TotFee += txInput.AmtFee
		}

		cust.CntCTot++
		acct.CntTot++

		// fmt.Println("about to map write")
		// upadte the cache for customer and account
		custAttMap[txInput.CustID]["totCDebit"] = cust.TotCDebit
		custAttMap[txInput.CustID]["totCCredit"] = cust.TotCCredit
		custAttMap[txInput.CustID]["totCFee"] = int64(cust.TotFee)
		custAttMap[txInput.CustID]["cntCTot"] = int64(cust.CntCTot)
		custAttMap[txInput.CustID]["cntCDebit"] = int64(cust.CntCDebit)
		custAttMap[txInput.CustID]["cntCCredit"] = int64(cust.CntCCredit)

		acctAttMap[txInput.AcctID]["totDebit"] = acct.TotDebit
		acctAttMap[txInput.AcctID]["totCredit"] = acct.TotCredit
		acctAttMap[txInput.AcctID]["totFee"] = int64(acct.TotFee)
		acctAttMap[txInput.AcctID]["cntTot"] = int64(acct.CntTot)
		acctAttMap[txInput.AcctID]["cntDebit"] = int64(acct.CntDebit)
		acctAttMap[txInput.AcctID]["cntCredit"] = int64(acct.CntCredit)

		custCache[txInput.CustID] = *cust
		acctCache[txInput.AcctID] = *acct

		mu.Unlock() // unlock the global map objects

		// send copies of customer and account objects through update channel

		txInfo := TxInfo{*txInput, rTxResult, *cust, *acct}
		// fmt.Printf("\nChannel input on result:\n%+v", txInfo)
		txChannel <- txInfo

	}()

	return rTxResult.ReturnStr
}

var scoreRank = map[string]int{"s": 1, "m": 2, "d": 3, "f": 4}

// processCustTests, returns the group results
func processCustTests(TstGrp *TestGrp, cID int64, aID int64, t time.Time, txInput *TxInput) []TestRes {
	var grpResult = make([]TestRes, 0)
	var wg0 sync.WaitGroup
	wg0.Add(len(TstGrp.Tests))
	chRes := make(chan TestRes, len(TstGrp.Tests))
	chSqlRes := make(chan [][]int64, 1)

	for toptststr, _ := range TstGrp.Tests { // one test at a time
		go func(tststr string, ch chan TestRes) {
			sqlstr := ""
			tRes := new(TestRes)
			var wg1 sync.WaitGroup

			tRes.TName = txTestCache[tststr].TName
			tRes.Categories = txTestCache[tststr].CategoryStr
			tRes.ParamStr = txTestCache[tststr].ParamStr

			defer wg0.Done()

			for idx, lparam := range txTestCache[tststr].Params { // process each param in the test, col is the columnSup struct
				switch lparam.QueryType {
				case 9: // aggregate type
					wg1.Add(1)
					go func(i1 int, tst string) {
						t2 := time.Now()
						sqlstm := "SELECT " + txTestCache[tst].Params[i1].SqlColStr + " FROM TX WHERE idCUST = ? and idTX > ? and " +
							txTestCache[tst].Params[i1].CName + ">0"
						defer wg1.Done()
						//t3 := time.Now()
						rows, err := db.Query(sqlstm, cID, txTestCache[tst].Period.getBegineTimeNano(t))
						//fmt.Printf("\nTime in Cust SQL Execution: %v, cid:%d, idTX: %d", time.Since(t3), cID, txTestCache[tst].Period.getBegineTimeNano(t))
						if err != nil {
							//fmt.Printf("\nmore errors here SQL, custid and time: %s, %d, %d ", sqlstm, cID, txTestCache[tst].Period.getBegineTimeNano(t))
							panic("\nerror in aggregate SQL result:" + err.Error() + "\n" + sqlstm + fmt.Sprintf(",params:%+v", txTestCache[tst].Params[1]))
						}

						if rows.Next() {
							rows.Scan(&TstGrp.TestParamValue[tst][i1])

							if TstGrp.TestParamValue[tst][i1] == nil {
								TstGrp.TestParamValue[tst][i1] = 0
							}

						} else {
							TstGrp.TestParamValue[tst][i1] = 0
						}
						switch txTestCache[tst].Params[i1].Scalar {
						case "MIN":
							var val int
							r := reflect.ValueOf(txInput)
							val = reflect.Indirect(r).FieldByName(txTestCache[tststr].Params[i1].TCName).Interface().(int)
							if TstGrp.TestParamValue[tst][i1].(int64) > int64(val) {
								TstGrp.TestParamValue[tst][i1] = val
							}
						case "MAX":
							var val int
							r := reflect.ValueOf(txInput)
							fmt.Printf("\nZero value?:%+v", txTestCache[tststr].Params[i1].TCName)
							val = reflect.Indirect(r).FieldByName(txTestCache[tststr].Params[i1].TCName).Interface().(int)
							if TstGrp.TestParamValue[tst][i1].(int64) < int64(val) {
								TstGrp.TestParamValue[tst][i1] = val
							}
						}
						rows.Close()
						fmt.Printf("\nTime in Cust Agg: %v, sql: %s", time.Since(t2), sqlstm)
						//fmt.Printf("\nSQL err: %v, value:%d, %f, sql:%s, cid%d, tim:e%d\n", err, TstGrp.TestSups[tststr][idx].SqlColValue, avgVal, sqlstm, cID, txTestCache[tststr].Period.getBegineTimeNano(t))
					}(idx, tststr)
				case 4, 5: // Sql type
					// do nothing

				case 1: // NoSQL type
					switch txTestCache[tststr].Params[idx].Scalar {
					case "t": // transaction field
						r := reflect.ValueOf(txInput)
						TstGrp.TestParamValue[tststr][idx] = reflect.Indirect(r).FieldByName(txTestCache[tststr].Params[idx].TCName).Interface()
						fmt.Printf("\nin case t:TCName:%s, \ninput:%+v, \nValue:%+v", txTestCache[tststr].Params[idx].TCName, txInput, TstGrp.TestParamValue[tststr][idx])
					case "c": // customer fields
						//fmt.Printf("\nin case c:%s, %+v, %+v", txTestCache[tststr].Params[idx].CName, custAttMap[cID], custAttMap[cID][txTestCache[tststr].Params[idx].CName])
						TstGrp.TestParamValue[tststr][idx] = custAttMap[cID][txTestCache[tststr].Params[idx].CName]
					case "a": // account fields
						//fmt.Printf("\nin case a:%s, %+v, %+v", txTestCache[tststr].Params[idx].CName, acctAttMap[aID], acctAttMap[aID][txTestCache[tststr].Params[idx].CName])
						TstGrp.TestParamValue[tststr][idx] = acctAttMap[aID][txTestCache[tststr].Params[idx].CName]
					}
				default:
					// dont' expect exceptions
					panic("\nUnexpected query type")
				}
			}
			wg1.Add(1)
			go func(sql string, tsts string) {
				//	t1 := time.Now()
				defer wg1.Done()

				// fmt.Printf("\nenter test%s", tsts)

				var sqlRes [][]int64

				sqlRes = <-chSqlRes
				// fmt.Printf("\nread from test%s", tsts)
				chSqlRes <- sqlRes // put right back to the channel for other test goroutines

				// fmt.Printf("\nresend from test%s", tsts)

				// fmt.Printf("\nChannel received:%+v", sqlRes)
				for idx, _ := range TstGrp.TestParamValue[tsts] { // assign column value
					if txTestCache[tsts].Params[idx].QueryType == 4 {
						switch txTestCache[tsts].Params[idx].Scalar {
						case "SUM":
							r := reflect.ValueOf(txInput)
							TstGrp.TestParamValue[tsts][idx] = custAttMap[cID][paramColMap[txTestCache[tsts].Params[idx].Param].CustCol].(int64) -
								sqlRes[TstGrp.SQLPeriodIdx[txTestCache[tsts].Period.Value].Idx][TstGrp.GrpParamIdx[txTestCache[tsts].Params[idx].SqlColStr]] +
								int64(reflect.Indirect(r).FieldByName(txTestCache[tsts].Params[idx].TCName).Interface().(int))

						case "CNT":
							TstGrp.TestParamValue[tsts][idx] = custAttMap[cID][paramColMap[txTestCache[tsts].Params[idx].Param].CustCol].(int64) -
								sqlRes[TstGrp.SQLPeriodIdx[txTestCache[tsts].Period.Value].Idx][TstGrp.GrpParamIdx[txTestCache[tsts].Params[idx].SqlColStr]] + 1

						}
					} else if txTestCache[tsts].Params[idx].QueryType == 5 { // avg function
						r := reflect.ValueOf(txInput)
						tVal := custAttMap[cID][paramColMap[txTestCache[tsts].Params[idx].Param].CustCol].(int64) -
							sqlRes[TstGrp.SQLPeriodIdx[txTestCache[tsts].Period.Value].Idx][TstGrp.GrpParamIdx[txTestCache[tsts].Params[idx].SqlColStr]] +
							int64(reflect.Indirect(r).FieldByName(txTestCache[tsts].Params[idx].TCName).Interface().(int))

						cntParamStr := "CNT(" + txTestCache[tsts].Params[idx].CName + ")"
						tCnt := custAttMap[cID][paramColMap[cntParamStr].CustCol].(int64) -
							sqlRes[TstGrp.SQLPeriodIdx[txTestCache[tsts].Period.Value].Idx][TstGrp.GrpParamIdx[paramColMap[cntParamStr].CustCol]] + 1

						if tCnt == 0 {
							TstGrp.TestParamValue[tsts][idx] = 0
						} else {
							TstGrp.TestParamValue[tsts][idx] = int64(tVal / tCnt)
						}
					}
				}

			}(sqlstr, tststr)
			// fmt.Printf("\ntstgrp 2:%+v \nCustCash:%+v\ncust att map:%+v", TstGrp, custCache[cID], custAttMap[cID])

			wg1.Wait()

			// fmt.Printf("\nGot here in Test: %s", tststr)

			// building test parameters
			paras := make(map[string]interface{})
			var strVal string
			// fmt.Printf("\nval in total TstGrp%+v\nParamValue:%+v", TstGrp, TstGrp.TestParamValue[tststr])

			for idx, val := range TstGrp.TestParamValue[tststr] {
				// fmt.Printf("\nval in TstGrp%v", val)
				paras["p"+strconv.Itoa(idx+1)] = val
				// fmt.Printf("\ninterface - 1 :%d, %v", txTestCache[tststr].Params[idx].Param, val.SqlColValue)

				//			strVal = strconv.FormatInt(val.SqlColValue.(int64), 10)
				// fmt.Printf("\nafter int type - 1 %v", val.SqlColValue)
				switch t := val.(type) {
				case string:
					strVal = val.(string)
				case int32, int64:
					strVal = strconv.FormatInt(val.(int64), 10)
				case int:
					strVal = strconv.Itoa(val.(int))
				default:
					fmt.Printf("\nwhat's going on?%v", t)
					panic("unknown type here")
				}

				if len(tRes.ParamValues) == 0 {
					tRes.ParamValues = strVal
				} else {
					tRes.ParamValues += "," + strVal
				}
			}

			//fmt.Printf("\nparameters: %+v", paras)

			// evaluate test results
			tRes.Score = "s"

			for scoreletter, mapstr := range txTestCache[tststr].ScoreMap {
				expression, err := govaluate.NewEvaluableExpression(mapstr)
				eval, err := expression.Evaluate(paras)
				if err != nil {
					panic("error evaluate expressions")
				}

				if eval.(bool) {
					if scoreRank[scoreletter] > scoreRank[tRes.Score] {
						tRes.Score = scoreletter
					}
				}
			}

			if len(tRes.Score) != 1 {
				tRes.Score = "e" // default, exception, consider as pass
			}

			ch <- *tRes

		}(toptststr, chRes)
	}

	// get SQL results
	if len(TstGrp.SQLStr) > 0 {
		var sqlstm string
		t1 := time.Now()
		ii := 0
		whereStr := "WHERE idCUST = " + strconv.FormatInt(cID, 10)

		for per, perIdx := range TstGrp.SQLPeriodIdx {
			sqlstm += "(SELECT " + TstGrp.SQLStr + " FROM TX " + whereStr + " and idTX > " +
				strconv.FormatInt(perIdx.Period.getBegineTimeNano(t), 10) + " ORDER BY idTX ASC LIMIT 1) UNION ALL (SELECT " + TstGrp.EmptyRowStr +
				" FROM (SELECT EXISTS (SELECT 1 FROM TX " + whereStr + " and idTX > " +
				strconv.FormatInt(perIdx.Period.getBegineTimeNano(t), 10) + " LIMIT 1) as c) as t where t.c = 0) UNION ALL "

			TstGrp.SQLPeriodIdx[per].Idx = ii // set group period index
			ii++
		}

		sqlstm = sqlstm[:len(sqlstm)-11] + ";" // remove the UNION at the end

		// fmt.Printf("\nsql group CUST sql: %s", sqlstm)
		rows, err := db.Query(sqlstm)

		if err != nil {
			log.Fatal(err)
		}

		sqlRes := make([][]int64, 0)
		for idx, _ := range sqlRes {
			sqlRes[idx] = make([]int64, TstGrp.SQLColIdx)
		}

		for rows.Next() {
			values := make([]int64, TstGrp.SQLColIdx)
			valuePtrs := make([]interface{}, TstGrp.SQLColIdx)

			for i := 0; i < TstGrp.SQLColIdx; i++ {
				valuePtrs[i] = &values[i]
			}

			err = rows.Scan(valuePtrs...) // retrieve column values

			sqlRes = append(sqlRes, values)
		}

		rows.Close()

		// fmt.Printf("\nCust Sql Result sent in channel:%+v", sqlRes)
		chSqlRes <- sqlRes
		// fmt.Printf("\nTime SQL:%s", sqlstm)

		fmt.Printf("\ntime elapse in Cust SQL query:%v", time.Since(t1))

	}
	// fmt.Printf("\nGot here, n tests:%d", len(TstGrp.Tests))
	for ii := 0; ii < len(TstGrp.Tests); ii++ {
		grpResult = append(grpResult, <-chRes)
	}

	wg0.Wait()
	close(chRes)
	close(chSqlRes)

	return grpResult
}

// processAcctTests, returns the group results
func processAcctTests(TstGrp *TestGrp, cID int64, aID int64, t time.Time, txInput *TxInput) []TestRes {
	var grpResult = make([]TestRes, 0)
	var wg0 sync.WaitGroup
	wg0.Add(len(TstGrp.Tests))
	chRes := make(chan TestRes, len(TstGrp.Tests))
	chSqlRes := make(chan [][]int64, 1)

	for toptststr, _ := range TstGrp.Tests { // one test at a time
		go func(tststr string, ch chan TestRes) {
			sqlstr := ""
			tRes := new(TestRes)
			var wg1 sync.WaitGroup

			tRes.TName = txTestCache[tststr].TName
			tRes.Categories = txTestCache[tststr].CategoryStr
			tRes.ParamStr = txTestCache[tststr].ParamStr

			defer wg0.Done()

			for idx, lparam := range txTestCache[tststr].Params { // process each param in the test, col is the columnSup struct
				switch lparam.QueryType {
				case 9: // aggregate type
					wg1.Add(1)
					go func(i1 int, tst string) {
						t2 := time.Now()
						sqlstm := "SELECT " + txTestCache[tst].Params[i1].SqlColStr + " FROM TX WHERE idCUST = ? and idACCT = ? and idTX > ? and " +
							txTestCache[tst].Params[i1].CName + ">0"
						defer wg1.Done()
						rows, err := db.Query(sqlstm, cID, aID, txTestCache[tst].Period.getBegineTimeNano(t))
						//fmt.Printf("\nTime in Acct SQL Execution: %v, cid:%d, idTX: %d", time.Since(t3), cID, txTestCache[tst].Period.getBegineTimeNano(t))
						if err != nil {
							//fmt.Printf("\nmore errors here SQL, acctid, acctid and time: %s, %d, %d %d ", sqlstm, cID,aID, txTestCache[tst].Period.getBegineTimeNano(t))
							panic("\nerror in aggregate SQL result:" + err.Error() + "\n" + sqlstm + fmt.Sprintf(",params:%+v", txTestCache[tst].Params[1]))
						}

						if rows.Next() {
							rows.Scan(&TstGrp.TestParamValue[tst][i1])
							if TstGrp.TestParamValue[tst][i1] == nil {
								TstGrp.TestParamValue[tst][i1] = 0
							}

						} else {
							TstGrp.TestParamValue[tst][i1] = 0
						}

						switch txTestCache[tst].Params[i1].Scalar {
						case "MIN":
							var val int
							r := reflect.ValueOf(txInput)
							val = reflect.Indirect(r).FieldByName(txTestCache[tststr].Params[i1].TCName).Interface().(int)
							if TstGrp.TestParamValue[tst][i1].(int64) > int64(val) {
								TstGrp.TestParamValue[tst][i1] = val
							}
						case "MAX":
							var val int
							r := reflect.ValueOf(txInput)
							val = reflect.Indirect(r).FieldByName(txTestCache[tststr].Params[i1].TCName).Interface().(int)
							if TstGrp.TestParamValue[tst][i1].(int64) < int64(val) {
								TstGrp.TestParamValue[tst][i1] = val
							}
						}

						rows.Close()
						fmt.Printf("\nTime in Acct Agg: %v, sql: %s", time.Since(t2), sqlstm)
						//fmt.Printf("\nSQL err: %v, value:%d, %f, sql:%s, cid%d, tim:e%d\n", err, TstGrp.TestSups[tststr][idx].SqlColValue, avgVal, sqlstm, cID, txTestCache[tststr].Period.getBegineTimeNano(t))
					}(idx, tststr)
				case 4, 5: // Sql type
					// do nothing

				case 1: // NoSQL type
					switch txTestCache[tststr].Params[idx].Scalar {
					case "t": // transaction field
						r := reflect.ValueOf(txInput)
						TstGrp.TestParamValue[tststr][idx] = reflect.Indirect(r).FieldByName(txTestCache[tststr].Params[idx].TCName).Interface()
						fmt.Printf("\nin case t:TCName:%s, \ninput:%+v, \nValue:%+v", txTestCache[tststr].Params[idx].TCName, txInput, TstGrp.TestParamValue[tststr][idx])
					case "c": // customer fields
						//fmt.Printf("\nin case c:%s, %+v, %+v", txTestCache[tststr].Params[idx].CName, custAttMap[cID], custAttMap[cID][txTestCache[tststr].Params[idx].CName])
						TstGrp.TestParamValue[tststr][idx] = custAttMap[cID][txTestCache[tststr].Params[idx].CName]
					case "a": // account fields
						//fmt.Printf("\nin case a:%s, %+v, %+v", txTestCache[tststr].Params[idx].CName, acctAttMap[aID], acctAttMap[aID][txTestCache[tststr].Params[idx].CName])
						TstGrp.TestParamValue[tststr][idx] = acctAttMap[aID][txTestCache[tststr].Params[idx].CName]
					}
				default:
					// dont' expect exceptions
					panic("\nUnexpected query type")
				}
			}
			wg1.Add(1)
			go func(sql string, tsts string) {
				//	t1 := time.Now()
				defer wg1.Done()

				// fmt.Printf("\nenter test%s", tsts)

				var sqlRes [][]int64

				sqlRes = <-chSqlRes
				// fmt.Printf("\nread from test%s", tsts)
				chSqlRes <- sqlRes // put right back to the channel for other test goroutines

				// fmt.Printf("\nresend from test%s", tsts)

				// fmt.Printf("\nChannel received:%+v", sqlRes)
				for idx, _ := range TstGrp.TestParamValue[tsts] { // assign column value
					if txTestCache[tsts].Params[idx].QueryType == 4 {
						switch txTestCache[tsts].Params[idx].Scalar {
						case "SUM":
							r := reflect.ValueOf(txInput)
							TstGrp.TestParamValue[tsts][idx] = acctAttMap[aID][paramColMap[txTestCache[tsts].Params[idx].Param].AcctCol].(int64) -
								sqlRes[TstGrp.SQLPeriodIdx[txTestCache[tsts].Period.Value].Idx][TstGrp.GrpParamIdx[txTestCache[tsts].Params[idx].SqlColStr]] +
								int64(reflect.Indirect(r).FieldByName(txTestCache[tsts].Params[idx].TCName).Interface().(int))

						case "CNT":
							TstGrp.TestParamValue[tsts][idx] = acctAttMap[aID][paramColMap[txTestCache[tsts].Params[idx].Param].AcctCol].(int64) -
								sqlRes[TstGrp.SQLPeriodIdx[txTestCache[tsts].Period.Value].Idx][TstGrp.GrpParamIdx[txTestCache[tsts].Params[idx].SqlColStr]] + 1

						}
					} else if txTestCache[tsts].Params[idx].QueryType == 5 {
						r := reflect.ValueOf(txInput)
						tVal := acctAttMap[aID][paramColMap[txTestCache[tsts].Params[idx].Param].AcctCol].(int64) -
							sqlRes[TstGrp.SQLPeriodIdx[txTestCache[tsts].Period.Value].Idx][TstGrp.GrpParamIdx[txTestCache[tsts].Params[idx].SqlColStr]] +
							int64(reflect.Indirect(r).FieldByName(txTestCache[tsts].Params[idx].TCName).Interface().(int))

						cntParamStr := "CNT(" + txTestCache[tsts].Params[idx].CName + ")"
						tCnt := acctAttMap[aID][paramColMap[cntParamStr].AcctCol].(int64) -
							sqlRes[TstGrp.SQLPeriodIdx[txTestCache[tsts].Period.Value].Idx][TstGrp.GrpParamIdx[paramColMap[cntParamStr].AcctCol]] + 1
						if tCnt == 0 {
							TstGrp.TestParamValue[tsts][idx] = 0
						} else {
							TstGrp.TestParamValue[tsts][idx] = int64(tVal / tCnt)
						}
					}
				}

			}(sqlstr, tststr)
			// fmt.Printf("\ntstgrp 2:%+v \nAcctCash:%+v\nAcct att map:%+v", TstGrp, acctCache[cID], acctAttMap[cID])

			wg1.Wait()

			// fmt.Printf("\nGot here in Test: %s", tststr)

			// building test parameters
			paras := make(map[string]interface{})
			var strVal string

			for idx, val := range TstGrp.TestParamValue[tststr] {
				paras["p"+strconv.Itoa(idx+1)] = val
				// fmt.Printf("\ninterface - 1 :%d, %v", txTestCache[tststr].Params[idx].Param, val.SqlColValue)

				//			strVal = strconv.FormatInt(val.SqlColValue.(int64), 10)
				// fmt.Printf("\nafter int type - 1 %v", val.SqlColValue)
				switch t := val.(type) {
				case string:
					strVal = val.(string)
				case int32, int64:
					strVal = strconv.FormatInt(val.(int64), 10)
				case int:
					strVal = strconv.Itoa(val.(int))
				default:
					fmt.Printf("\nwhat's going on?%v", t)
					panic("unknown type here")
				}

				if len(tRes.ParamValues) == 0 {
					tRes.ParamValues = strVal
				} else {
					tRes.ParamValues += "," + strVal
				}
			}

			//fmt.Printf("\nparameters: %+v", paras)

			// evaluate test results
			tRes.Score = "s"

			for scoreletter, mapstr := range txTestCache[tststr].ScoreMap {
				expression, err := govaluate.NewEvaluableExpression(mapstr)
				eval, err := expression.Evaluate(paras)
				if err != nil {
					panic("error evaluate expressions")
				}

				if eval.(bool) {
					if scoreRank[scoreletter] > scoreRank[tRes.Score] {
						tRes.Score = scoreletter
					}
				}
			}

			if len(tRes.Score) != 1 {
				tRes.Score = "e" // default, exception, consider as pass
			}

			ch <- *tRes

		}(toptststr, chRes)
	}

	// get SQL results
	if len(TstGrp.SQLStr) > 0 {
		var sqlstm string
		t1 := time.Now()
		ii := 0
		whereStr := "WHERE idCUST = " + strconv.FormatInt(cID, 10) + " and idACCT = " + strconv.FormatInt(aID, 10)

		for per, perIdx := range TstGrp.SQLPeriodIdx {
			sqlstm += "(SELECT " + TstGrp.SQLStr + " FROM TX " + whereStr + " and idTX > " +
				strconv.FormatInt(perIdx.Period.getBegineTimeNano(t), 10) + " ORDER BY idTX ASC LIMIT 1) UNION ALL (SELECT " + TstGrp.EmptyRowStr +
				" FROM (SELECT EXISTS (SELECT 1 FROM TX " + whereStr + " and idTX > " +
				strconv.FormatInt(perIdx.Period.getBegineTimeNano(t), 10) + " LIMIT 1) as c) as t where t.c = 0) UNION ALL "

			TstGrp.SQLPeriodIdx[per].Idx = ii // set group period index
			ii++
		}

		sqlstm = sqlstm[:len(sqlstm)-11] + ";" // remove the UNION at the end

		// fmt.Printf("\nsql group Acct sql: %s", sqlstm)
		rows, err := db.Query(sqlstm)

		if err != nil {
			log.Fatal(err)
		}

		sqlRes := make([][]int64, 0)
		for idx, _ := range sqlRes {
			sqlRes[idx] = make([]int64, TstGrp.SQLColIdx)
		}

		for rows.Next() {
			values := make([]int64, TstGrp.SQLColIdx)
			valuePtrs := make([]interface{}, TstGrp.SQLColIdx)

			for i := 0; i < TstGrp.SQLColIdx; i++ {
				valuePtrs[i] = &values[i]
			}

			err = rows.Scan(valuePtrs...) // retrieve column values

			sqlRes = append(sqlRes, values)
		}

		rows.Close()

		// fmt.Printf("\nAcct Sql Result sent in channel:%+v", sqlRes)
		chSqlRes <- sqlRes
		// fmt.Printf("\nTime SQL:%s", sqlstm)

		fmt.Printf("\ntime elapse in Acct SQL query:%v", time.Since(t1))

	}
	// fmt.Printf("\nGot here, n tests:%d", len(TstGrp.Tests))
	for ii := 0; ii < len(TstGrp.Tests); ii++ {
		grpResult = append(grpResult, <-chRes)
	}

	wg0.Wait()
	close(chRes)
	close(chSqlRes)

	return grpResult
}

// Perform database updates on a transaction
// Affected tables are: TX, CUST, ACCT, TXVERIFY
// Tx input data are obtained
func updateTX() {

	// forever monitor and reading the txChannel
	nTx := 0
	for {
		//	time.Sleep(time.Duration(2) * time.Second)
		fmt.Printf("\nChannel size:%d", len(txChannel))
		txInfo, ok := <-txChannel

		txGrp := []TxInfo{txInfo}

		if !ok {
			fmt.Println("Channel closed!")
			break
		}

		b := 0 // break flag
		nTx = 1
		// perform batch update up to 200 records at a time, 199 because one is alread read
		// parameter to be configured.
		for i := 0; i < 199; i++ {

			select {
			case txInfo, ok := <-txChannel:
				if ok {
					// fmt.Printf("\nSubsequent gets from channel:\n%+v\n", txInfo)
					txGrp = append(txGrp, txInfo)
					nTx++
				} else {
					fmt.Println("Channel closed!")
					b = 1
				}

			default:
				b = 1
			}

			if b == 1 {
				break
			}

		}

		// fmt.Printf("\nTotal record from channel:%d", nTx)
		updateTXGrp(txGrp)
	}

}

// Persist a group transaction info to the database
// Update the TX, CUST, ACCT, and TXVERIFY tables
func updateTXGrp(txGrp []TxInfo) {

	sqlTX := "INSERT INTO TX (idCUST,idTX,idHostTX,idACCT,idFI,txDatetime," +
		"amtDebit,amtFee,totFee,totDebit,amtCredit,totCredit,balACCT,txCode,cntDebit,cntCredit,cntTot,balCUST,totCDebit,totCCredit,totCFee,cntCDebit,cntCCredit,cntCTot," +
		"verifyFlag,sumScore,safes,fails,dangers,maybes) VALUES "
	sqlTestRes := "INSERT INTO TXTESTRES (idCUST, idTX, name, paramValues, score) VALUES "
	sqlTestRet := "INSERT INTO TXTESTRET (idCUST, idTX, definedFilters, activeFilters, txReturn) VALUES "
	sqlCust := "INSERT INTO CUST (idCUST,totCDebit,totCCredit,totFee,bal,cntCDebit,cntCCredit,cntCTot) VALUES "
	sqlAcct := "INSERT INTO ACCT (idCUST,idACCT,totDebit,totCredit,totFee,bal,cntDebit,cntCredit,cntTot) VALUES "

	vTested := false

	for _, txInfo := range txGrp {
		// optimization opportunity, reduce the redundant ctrconv() on the same values
		sqlTX += "(" + strconv.FormatInt(txInfo.TxIn.CustID, 10) + "," + strconv.FormatInt(txInfo.TxIn.TxID, 10) + ",'" +
			txInfo.TxIn.HostTxID + "'," + strconv.FormatInt(txInfo.TxIn.AcctID, 10) + "," +
			strconv.Itoa(txInfo.TxIn.FiID) + ",'" + txInfo.TxIn.TxDateTime + "'," +
			strconv.Itoa(txInfo.TxIn.AmtDebit) + "," + strconv.Itoa(txInfo.TxIn.AmtFee) + "," + strconv.Itoa(txInfo.TxAcct.TotFee) + "," +
			strconv.FormatInt(txInfo.TxAcct.TotDebit, 10) + "," + strconv.Itoa(txInfo.TxIn.AmtCredit) + "," +
			strconv.FormatInt(txInfo.TxAcct.TotCredit, 10) + "," + strconv.FormatInt(txInfo.TxAcct.Bal, 10) + "," +
			strconv.Itoa(txInfo.TxIn.TxCode) + "," + strconv.Itoa(txInfo.TxAcct.CntDebit) + "," + strconv.Itoa(txInfo.TxAcct.CntCredit) + "," +
			strconv.Itoa(txInfo.TxAcct.CntTot) + "," +
			strconv.FormatInt(txInfo.TxCust.Bal, 10) + "," + strconv.FormatInt(txInfo.TxCust.TotCDebit, 10) + "," +
			strconv.FormatInt(txInfo.TxCust.TotCCredit, 10) + "," + strconv.Itoa(txInfo.TxCust.TotFee) + "," +
			strconv.Itoa(txInfo.TxCust.CntCDebit) + "," + strconv.Itoa(txInfo.TxCust.CntCCredit) + "," +
			strconv.Itoa(txInfo.TxCust.CntCTot) + "," + strconv.Itoa(txInfo.TxIn.VerifyFlag) + ",'" + txInfo.TestRes.SumScore + "'," +
			strconv.Itoa(txInfo.TestRes.Safes) + "," + strconv.Itoa(txInfo.TestRes.Fails) + "," +
			strconv.Itoa(txInfo.TestRes.Dangers) + "," + strconv.Itoa(txInfo.TestRes.Maybes) + "), "

		for _, txTest := range txInfo.TestRes.TestResults {
			sqlTestRes += "(" + strconv.FormatInt(txInfo.TxIn.CustID, 10) + "," + strconv.FormatInt(txInfo.TxIn.TxID, 10) + ",'" + txTest.TName + "','" +
				txTest.ParamValues + "','" + txTest.Score + "'), "
			vTested = true
		}

		sqlTestRet += "(" + strconv.FormatInt(txInfo.TxIn.CustID, 10) + "," + strconv.FormatInt(txInfo.TxIn.TxID, 10) + ",'" +
			txInfo.TestRes.DefinedFilters + "','" + txInfo.TestRes.ActiveFilters + "','" + txInfo.TestRes.ReturnStr + "'), "
	}

	//trim the last ,
	sqlTX = sqlTX[0:len(sqlTX)-2] + " on duplicate key update special='D';"
	// fmt.Printf("\nTxGrp: %+v\nTX Insert:\n%s", txGrp, sqlTX)
	_, err := db.Exec(sqlTX)
	if err != nil {
		panic("\nTX Insert Statement error\n" + err.Error()) // proper error handling instead of panic in your app
	}

	if vTested {
		sqlTestRes = sqlTestRes[0:len(sqlTestRes)-2] + " on duplicate key update info='dup';"
		//fmt.Printf("\ntest Inserts:\n%s", sqlTestRes)
		_, err := db.Exec(sqlTestRes)
		if err != nil {
			panic("\nTXTESTRES Insert Statement error\n" + err.Error()) // proper error handling instead of panic in your app
		}
	}

	sqlTestRet = sqlTestRet[0:len(sqlTestRet)-2] + " on duplicate key update exception='dup';"
	//fmt.Printf("\nretrun string:\n%s", sqlTestRet)
	_, err = db.Exec(sqlTestRet)
	if err != nil {
		panic("\nTXTESTRET Insert Statement error\n" + err.Error()) // proper error handling instead of panic in your app
	}

	// merge duplicate customer and account records
	var custMerge CustSlice
	var acctMerge AcctSlice
	dim := len(txGrp)
	var tmpTxInfo TxInfo

	for i := 0; i < dim; i++ {
		if txGrp[i].TxCust.Dup == 0 { // check if the item is already processed. If it has do nothing
			tmpTxInfo = txGrp[i]
			for j := i + 1; j < dim; j++ {
				if txGrp[j].TxCust.Dup == 0 && tmpTxInfo.TxCust.CustID == txGrp[j].TxCust.CustID {
					txGrp[j].TxCust.Dup = 1
					if tmpTxInfo.TxIn.TxID < txGrp[j].TxIn.TxID {
						tmpTxInfo = txGrp[j]
					}
				}
			}
			custMerge = append(custMerge, tmpTxInfo.TxCust)
		}
	}

	for i := 0; i < dim; i++ {
		if txGrp[i].TxAcct.Dup == 0 { // check if the item is already processed. If it has do nothing
			tmpTxInfo = txGrp[i]
			for j := i + 1; j < dim; j++ {
				if txGrp[j].TxAcct.Dup == 0 && tmpTxInfo.TxAcct.AcctID == txGrp[j].TxAcct.AcctID {
					txGrp[j].TxAcct.Dup = 1
					if tmpTxInfo.TxIn.TxID < txGrp[j].TxIn.TxID {
						tmpTxInfo = txGrp[j]
					}
				}
			}
			acctMerge = append(acctMerge, tmpTxInfo.TxAcct)
		}
	}

	// sort customers and accounts to avoid deadlocks
	sort.Sort(custMerge)
	sort.Sort(acctMerge)

	for _, custRec := range custMerge {
		sqlCust += "(" + strconv.FormatInt(custRec.CustID, 10) + "," + strconv.FormatInt(custRec.TotCDebit, 10) + "," +
			strconv.FormatInt(custRec.TotCCredit, 10) + "," + strconv.Itoa(custRec.TotFee) + "," +
			strconv.FormatInt(custRec.Bal, 10) + "," +
			strconv.Itoa(custRec.CntCDebit) + "," + strconv.Itoa(custRec.CntCCredit) + "," +
			strconv.Itoa(custRec.CntCTot) + "), "
	}

	for _, acctRec := range acctMerge {
		sqlAcct += "(" + strconv.FormatInt(acctRec.CustID, 10) + "," + strconv.FormatInt(acctRec.AcctID, 10) + "," +
			strconv.FormatInt(acctRec.TotDebit, 10) + "," +
			strconv.FormatInt(acctRec.TotCredit, 10) + "," + strconv.Itoa(acctRec.TotFee) + "," +
			strconv.FormatInt(acctRec.Bal, 10) + "," +
			strconv.Itoa(acctRec.CntDebit) + "," + strconv.Itoa(acctRec.CntCredit) + "," +
			strconv.Itoa(acctRec.CntTot) + "), "
	}

	sqlCust = sqlCust[0:len(sqlCust)-2] + " ON DUPLICATE KEY UPDATE " +
		"TotCDebit=VALUES(TotCDebit),TotCCredit=VALUES(TotCCredit),TotFee=VALUES(TotFee),Bal=VALUES(Bal)," +
		"CntCDebit=VALUES(CntCDebit),CntCCredit=VALUES(CntCCredit),CntCTot=VALUES(CntCTot);"
	//fmt.Printf("\nCust-- Insert:\n%s", sqlCust)
	_, err = db.Exec(sqlCust)
	if err != nil {
		panic("\nCustomer Insert Statement error\n" + err.Error()) // proper error handling instead of panic in your app
	}

	sqlAcct = sqlAcct[0:len(sqlAcct)-2] + " ON DUPLICATE KEY UPDATE " +
		"TotDebit=VALUES(TotDebit),TotCredit=VALUES(TotCredit),TotFee=VALUES(TotFee),Bal=VALUES(Bal)," +
		"CntDebit=VALUES(CntDebit),CntCredit=VALUES(CntCredit),CntTot=VALUES(CntTot);"
	//fmt.Printf("\nAcct Insert:\n%s", sqlAcct)
	_, err = db.Exec(sqlAcct)
	if err != nil {
		panic("\nAccout Insert Statement error\n" + err.Error()) // proper error handling instead of panic in your app
	}

}
