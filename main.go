package main
import (
	"bytes"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"
    "gopkg.in/mgo.v2"
    "time"
    "os"
    "io"
    "strconv"
    "strings"
    "sync"
    "path/filepath"
	 )
type FilesDetails struct {
    UniqueId        string
    FileStructure   string
    ObjClass        string
    ObjType         string
    ObjCategory     string
    URL             string
    UploadTimestamp string
    Filename        string
    Version         int
    DisplayName     string
    CompanyId       int
    TenantId        int
    RefId           string
    Status          string
    Size            int
    CreatedAt       string `json:"createdAt"`
    UpdatedAt       string `json:"updatedAt"`
    ApplicationId   string
    FileCategoryId  int
}
type count struct {
    Exception     string
    CustomMessage string
    IsSuccess     bool
    Result        int
}
type mongofile struct {

}
type Respond struct {
    Exception     string
    CustomMessage string
    IsSuccess     bool
    Result        []FilesDetails
}
type Item struct {
    CompanyId  int
    SpaceLimit Limit
}
type Limit struct{
    SpaceLimit int
    SpaceType  string
    SpaceUnit  string
}
type Config struct {
    RootPath string
    ConcFilecount int
    Services struct {
             AccessToken        string
             UserServiceHost    string
             UserServicePort    string
             UserServiceVersion string
         }
    Mongo struct {
             Mhost        string
             Mdb    string
             Musername    string
             Mpassword string
         }
}
func main() {
    t := time.Now()
   // fmt.Println(t.String())
    starttime:=t.Format("2006-01-02 15:04:05")
    conf := loadConfig()
    //fmt.Println(conf.Mongo.Mhost,conf.Mongo.Musername,conf.Mongo.Mpassword)
    rootPath := conf.RootPath
    accessToken := conf.Services.AccessToken
    host:=conf.Services.UserServiceHost
    concFilecount:=conf.ConcFilecount
    //mongo Connection
    info := &mgo.DialInfo{
        Addrs:    []string{conf.Mongo.Mhost},
        Timeout:  60 * time.Second,
        Database: conf.Mongo.Mdb,
        Username: conf.Mongo.Musername,
        Password: conf.Mongo.Mpassword,
    }
    session, err1 := mgo.DialWithInfo(info)
    if err1 != nil {
        panic(err1)
    }
    db := mgo.Database{}
    db.Session = session
    db.Name = conf.Mongo.Mdb

        //"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJzdWtpdGhhIiwianRpIjoiYWEzOGRmZWYtNDFhOC00MWUyLTgwMzktOTJjZTY0YjM4ZDFmIiwic3ViIjoiNTZhOWU3NTlmYjA3MTkwN2EwMDAwMDAxMjVkOWU4MGI1YzdjNGY5ODQ2NmY5MjExNzk2ZWJmNDMiLCJleHAiOjE5MDIzODExMTgsInRlbmFudCI6LTEsImNvbXBhbnkiOi0xLCJzY29wZSI6W3sicmVzb3VyY2UiOiJhbGwiLCJhY3Rpb25zIjoiYWxsIn1dLCJpYXQiOjE0NzAzODExMTh9.Gmlu00Uj66Fzts-w6qEwNUz46XYGzE8wHUhAJOFtiRo"
    authToken := fmt.Sprintf("Bearer %s", accessToken)

    fmt.Println("press 1 for \"all files category wise\"")
    fmt.Println("press 2 for \"all files ,all category,date range wise\"")
    fmt.Println("press 3 for \"all files category and date range wise \"")
    fmt.Print("enter your selection : ")
    var selection string
    fmt.Scanln(&selection)

    i, err := strconv.Atoi(selection)
    checkErr(err)
    
   
    if(1==i){//all files category wise 
        CategoryArray:=createCategoryArray()
        //fmt.Println(CategoryArray)
        url:=setCountUrl(false,host,"","")
       // fmt.Println("count: ",url)
        data := make(map[string]interface{})
        data["categoryList"] = CategoryArray
        bytearray, err := json.Marshal(data)
        //atlist=["abc","bbc"]
        var CatjsonStr = []byte(bytearray)
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(CatjsonStr))
        req.Header.Set("Authorization", authToken)
        req.Header.Set("Content-Type", "application/json")

        client := &http.Client{}
        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        defer resp.Body.Close()
        body, _ := ioutil.ReadAll(resp.Body)
        rep := count{}
        err = json.Unmarshal(body, &rep)
        fmt.Println("Reord Count :" ,rep.Result)

        //fmt.Println(rep.Result / concFilecount)
       
        for i := 1; i <= ((rep.Result / concFilecount) + 1); i++ {

            url:=setRecodsUrl(i,concFilecount,false,host,"","")
            //fmt.Println("records: ",url)
            fileWrite(rootPath,getRecodes(url,host,authToken,CatjsonStr),db)
        }


    }else if (2==i){//all files in all category daterange wise 
        startd, endd:=getDateRange()
    	//fmt.Println(startd,endd)
        url:=setCountUrl(true,host,startd,endd)
        //fmt.Println("count: ",url)
        var jsonStr = []byte(`{"categoryList":["TICKET_ATTACHMENTS","CONVERSATION","REPORTS","AGENT_GREETINGS","IVRCLIPS","PROFILE_PICTURES","NOTICE_ATTACHMENTS"]}`)
        //r jsonStr = []byte(`{"categoryList":["PROFILE_PICTURES","TICKET_ATTACHMENTS","CONVERSATION","REPORTS","AGENT_GREETINGS"]}`)
        //var jsonStr = []byte(`{"categoryList":["PROFILE_PICTURES","TICKET_ATTACHMENTS","CONVERSATION","REPORTS","AGENT_GREETINGS"]}`)
        //req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))//data := make(map[string]interface{})
        //data["categoryList"] = CategoryArray
        //bytearray, err := json.Marshal(data)
        //var CatjsonStr = []byte(jsonStr)
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
        req.Header.Set("Authorization", authToken)
        req.Header.Set("Content-Type", "application/json")

        client := &http.Client{}
        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        defer resp.Body.Close()
        body, _ := ioutil.ReadAll(resp.Body)
        rep := count{}
        err = json.Unmarshal(body, &rep)
        fmt.Println("Reord Count :" ,rep.Result)


        //fmt.Println(rep.Result / concFilecount)
       
        for i := 1; i <= ((rep.Result / concFilecount) + 1); i++ {

            url:=setRecodsUrl(i,concFilecount,false,host,"","")
            //fmt.Println("records: ",url)
            fileWrite(rootPath,getRecodes(url,host,authToken,jsonStr),db)
        }
    }else if (3==i){//all file category and daterange wise 
        startd, endd:=getDateRange()
        CategoryArray:=createCategoryArray()
        url:=setCountUrl(true,host,startd,endd)
        //fmt.Println("count: ",url)
        data := make(map[string]interface{})
        data["categoryList"] = CategoryArray
        bytearray, err := json.Marshal(data)
        var CatjsonStr = []byte(bytearray)
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(CatjsonStr))
        req.Header.Set("Authorization", authToken)
        req.Header.Set("Content-Type", "application/json")

        client := &http.Client{}
        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        defer resp.Body.Close()
        body, _ := ioutil.ReadAll(resp.Body)
        rep := count{}
        err = json.Unmarshal(body, &rep)
        fmt.Println("Reord Count :" ,rep.Result)


        //fmt.Println(rep.Result / concFilecount)
       
        for i := 1; i <= ((rep.Result / concFilecount) + 1); i++ {

            url:=setRecodsUrl(i,concFilecount,false,host,"","")
            //fmt.Println("records: ",url)
            fileWrite(rootPath,getRecodes(url,host,authToken,CatjsonStr),db)
        }
    }else{
    	fmt.Println("sinhala berida oi ")
    }
     t = time.Now()
    //fmt.Println(t.String())
    endtime:=t.Format("2006-01-02 15:04:05")
    fmt.Println("All Done ..................")
    fmt.Println("start time :",starttime)
    fmt.Println("End time :",endtime)

}
func GetDirPath() string {
    envPath := os.Getenv("GO_CONFIG_DIR")
    if envPath == "" {
        envPath = "C:\\Users\\pamidu\\Desktop\\gotool"
    }
    //fmt.Println(envPath)

    envPath = filepath.Join(envPath,"config")

    return envPath
}
func loadConfig() Config{

    dirPath := GetDirPath()
    confPath := filepath.Join(dirPath, "default.json")
    //fmt.Println("GetDefaultConfig config path: ", confPath)
    content, operr := ioutil.ReadFile(confPath)
    if operr != nil {
        fmt.Println(operr)
    }

    defConfiguration := Config{}
    json.Unmarshal(content, &defConfiguration)

    ////////////////////////load envs/////////////////////////////////////
    envConfPath := filepath.Join(dirPath, "custom-environment-variables.json")

    envContent, operr := ioutil.ReadFile(envConfPath)


    if operr != nil {
        fmt.Println(operr)
    }else {

        defEnvConfiguration := Config{}
        unErr := json.Unmarshal(envContent, &defEnvConfiguration)

        //fmt.Println(defConfiguration)

        if unErr != nil {

            if defEnvConfiguration.Services.AccessToken != "" {

                defConfiguration.Services.AccessToken = os.Getenv(defEnvConfiguration.Services.AccessToken)
            }

            if defEnvConfiguration.Services.UserServiceHost != "" {

                defConfiguration.Services.UserServiceHost = os.Getenv(defEnvConfiguration.Services.UserServiceHost)
            }

            if defEnvConfiguration.Services.UserServiceVersion != "" {

                defConfiguration.Services.UserServiceVersion = os.Getenv(defEnvConfiguration.Services.UserServiceVersion)
            }

        }
    }


    return defConfiguration
}
func checkErr(err error) {
    if err != nil {
        fmt.Println(err)
    }
}
func createCategoryArray() []string{
     var catforPost []string
        //fmt.Println("selection 1")
        fmt.Println("press 1 for \"TICKET_ATTACHMENTS\" \npress 2 for \"CONVERSATION\" \npress 3 for \"REPORTS\" \npress 4 for \"AGENT_GREETINGS\" \npress 5 for \"IVRCLIPS\" \npress 6 for \"PROFILE_PICTURES\" \npress 7 for \"NOTICE_ATTACHMENTS\" ")
        fmt.Println("enter category list")
        fmt.Println("eg: 1,2,5")
        var categorylist string
        fmt.Scanln(&categorylist)
        s := strings.Split(categorylist, ",")
        //fmt.Println(s)
        for _, cat := range s {
            catint,_:=strconv.Atoi(cat)
            if(1==catint){
                catforPost = append(catforPost, "TICKET_ATTACHMENTS")
            }else if(2==catint){
                catforPost = append(catforPost, "CONVERSATION")
            }else if(3==catint){
                catforPost = append(catforPost, "REPORTS")
            }else if(4==catint){
                catforPost = append(catforPost, "AGENT_GREETINGS")
            }else if(5==catint){
                catforPost = append(catforPost, "IVRCLIPS")
            }else if(6==catint){
                catforPost = append(catforPost, "PROFILE_PICTURES")
            }else if(7==catint){
                catforPost = append(catforPost, "NOTICE_ATTACHMENTS")
            }
        }
        return catforPost

}
func getDateRange() (string,string){
    fmt.Println("Enter Stard Date")
    fmt.Println("eg: 2017-05-01")
    var startdate string
    fmt.Scanln(&startdate)
    fmt.Println("Enter End Date")
    fmt.Println("eg: 2017-07-01")
    var enddate string
    fmt.Scanln(&enddate)
    return startdate, enddate
}
func setCountUrl(daterange bool ,host string,stardate string,enddate string) string {
    url := ""
    if (daterange) {
        url = fmt.Sprintf("http://%s/DVP/API/1.0.0.0/FileService/FileInfo/ByCategoryList/count?startDateTime=%s&endDateTime=%s", host,stardate, enddate)
    }else {
        url = fmt.Sprintf("http://%s/DVP/API/1.0.0.0/FileService/FileInfo/ByCategoryList/count",host)

    }
    return url
    
}
func setRecodsUrl(i int ,concFilecount int,daterange bool ,host string ,stardate string,enddate string) string {
    url:=""
    if (daterange) {
            url = fmt.Sprintf("http://%s/DVP/API/1.0.0.0/FileService/FileInfo/ByCategoryList/%d/%d?startDateTime=%s&endDateTime=%s", host,concFilecount,i,stardate, enddate)
        }else {
            url = fmt.Sprintf("http://%s/DVP/API/1.0.0.0/FileService/FileInfo/ByCategoryList/%d/%d",host,concFilecount,i)
        }
    return url
}
func ParseDate4(date string) string {
    s := strings.Split(date, "-")
    year, month, date := s[0], s[1], s[2]
    date = date[0:2]
    path := year + "/" + month + "/" + date
    return path
}
func getRecodes(url string,host string,authToken string,CatjsonStr []byte  )Respond{
        //var jsonStr = []byte(`{"categoryList":["PROFILE_PICTURES","TICKET_ATTACHMENTS","CONVERSATION","REPORTS","AGENT_GREETINGS"]}`)
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(CatjsonStr))
        req.Header.Set("Authorization", authToken)
        req.Header.Set("Content-Type", "application/json")

        client := &http.Client{}
        resp, err := client.Do(req)
        checkErr(err)
        defer resp.Body.Close()
        body, _ := ioutil.ReadAll(resp.Body)
        rep := Respond{}
        err = json.Unmarshal(body, &rep)
        //fmt.Println("-----------------------------------------")
        //fmt.Println(rep.Result)
        //fmt.Println("-----------------------------------------")
        return rep
}
func fileWrite(rootPath string,rep Respond,db mgo.Database){
    var wg sync.WaitGroup

        wg.Add(len(rep.Result))

        for _, recods := range rep.Result {

             go func(recods FilesDetails) {
                 defer wg.Done()

                datepath := ParseDate4(recods.CreatedAt)
                fmt.Println(recods.FileStructure, recods.ObjClass, recods.Filename, recods.CreatedAt)
                // var dataset bson.M
                file, err := db.GridFS("fs").Open(recods.UniqueId)
                checkErr(err)
                path := (rootPath+ "/"+strconv.Itoa(recods.CompanyId) + "/" + strconv.Itoa(recods.TenantId) + "/" + datepath + "/")
                fmt.Println(path)
                //fmt.Println(ParseDate4(recods.CreatedAtre))
                if _, err := os.Stat(path); os.IsNotExist(err) {
                    os.MkdirAll(path, os.ModePerm)
                }
                if (file != nil) {
                    out, _ := os.Create(path + "/" + recods.Filename)
                    _, err = io.Copy(out, file)
                    checkErr(err)
                    err = file.Close()
                    out.Close()
                    checkErr(err)
                }
                if _, err := os.Stat(path + "/" + recods.Filename); !os.IsNotExist(err) {
                   removeFile(db,recods.UniqueId)
                   //fileWrite(rootPath,getRecodes(url,host,authToken,CatjsonStr),db)
                }
                //execResponses <- true
            }(recods)

        }

        wg.Wait()

}
func removeFile(db mgo.Database,UniqueId string){
    //err := db.GridFS("fs").Remove(UniqueId)
    //checkErr(err)
    //if(err == nil){
    if(true){
        updatePath()
    }
    fmt.Println("File Delete method :",UniqueId)


}
func updatePath(){
    fmt.Println("File path update method :")
}


