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
type categoryresult struct {
    id          int
    Owner       string
    Category    string
    Visible     bool
    Encripted   bool
    createdAt   string
    updatedAt   string
}
type Respondcatlist struct {
    Exception     string
    CustomMessage string
    IsSuccess     bool
    Result        []categoryresult
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
    fmt.Println(t.String())
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
    authToken := fmt.Sprintf("Bearer %s", accessToken)
    
    fmt.Println("Enter Company ID ")
    var cid string
    fmt.Scanln(&cid)
    
    fmt.Println("Enter Tenant ID")
    var tid string
    fmt.Scanln(&tid)
    
    fmt.Println("press 1 for \"all files category wise\"")
    fmt.Println("press 2 for \"all files ,all category,date range wise\"")
    fmt.Println("press 3 for \"all files category and date range wise \"")
    fmt.Print("enter your selection : ")
    var selection string
    fmt.Scanln(&selection)

    i, err := strconv.Atoi(selection)
    checkErr(err)

    fmt.Println("Do You Want to Delete files From MongoDB (Y/N)")
    var deleteconfirm string
    fmt.Scanln(&deleteconfirm)
    confirm:=false
    if("Y"==deleteconfirm){
        confirm=true
    }
    if(1==i){//all files category wise 
        CategoryArray:=createCategoryArray(host,authToken,tid,cid)
        url:=setCountUrl(false,host,"","")
        data := make(map[string]interface{})
        data["categoryList"] = CategoryArray
        bytearray, err := json.Marshal(data)
        var CatjsonStr = []byte(bytearray)
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(CatjsonStr))
        req.Header.Set("Authorization", authToken)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("companyinfo", tid+":"+cid)

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
        for i := 1; i <= ((rep.Result / concFilecount) + 1); i++ {
            url:=setRecodsUrl(i,concFilecount,false,host,"","")
            fileWrite(rootPath,getRecodes(url,host,authToken,tid,cid,CatjsonStr),confirm,db)
        }
    }else if (2==i){//all files in all category daterange wise 
        

        startd, endd:=getDateRange()
        url:=setCountUrl(true,host,startd,endd)


        catlisturl := fmt.Sprintf("http://%s/DVP/API/1.0.0.0/FileService/FileCategories",host)
        catlistreq, catlisterr := http.NewRequest("GET", catlisturl, nil)
        catlistreq.Header.Set("Authorization", authToken)
        catlistreq.Header.Set("Content-Type", "application/json")
        catlistreq.Header.Set("companyinfo", tid+":"+cid)

        catlistclient := &http.Client{}
        catlistresp, catlisterr := catlistclient.Do(catlistreq)
        if catlisterr != nil {
            panic(catlisterr)
        }
        defer catlistresp.Body.Close()
        catlistbody, _ := ioutil.ReadAll(catlistresp.Body)
        catlistrep := Respondcatlist{}
        err = json.Unmarshal(catlistbody, &catlistrep)
        var catlist []string
        index:=0
        for _, cat := range catlistrep.Result {
           fmt.Println("press ", index+1, " for " ,cat.Category)
           catlist = append(catlist, cat.Category)
           index++
        }
        data := make(map[string]interface{})
        data["categoryList"] = catlist
        bytearray, err := json.Marshal(data)
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(bytearray))
        req.Header.Set("Authorization", authToken)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("companyinfo", tid+":"+cid)

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
        for i := 1; i <= ((rep.Result / concFilecount) + 1); i++ {
            url:=setRecodsUrl(i,concFilecount,false,host,"","")
            fileWrite(rootPath,getRecodes(url,host,authToken,tid,cid,bytearray),confirm,db)
        }
    }else if (3==i){//all file category and daterange wise 
        startd, endd:=getDateRange()
        CategoryArray:=createCategoryArray(host,authToken,tid,cid)
        url:=setCountUrl(true,host,startd,endd)
        data := make(map[string]interface{})
        data["categoryList"] = CategoryArray
        bytearray, err := json.Marshal(data)
        var CatjsonStr = []byte(bytearray)
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(CatjsonStr))
        req.Header.Set("Authorization", authToken)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("companyinfo", tid+":"+cid)

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
        for i := 1; i <= ((rep.Result / concFilecount) + 1); i++ {
            url:=setRecodsUrl(i,concFilecount,false,host,"","")
            fileWrite(rootPath,getRecodes(url,host,authToken,tid,cid,CatjsonStr),confirm,db)
        }
    }else{
        fmt.Println("sinhala berida oi ")
    }
    t = time.Now()
    fmt.Println(t.String())
    endtime:=t.Format("2006-01-02 15:04:05")
    fmt.Println("All Done ..................")
    fmt.Println("start time :",starttime)
    fmt.Println("End time :",endtime)
}
func GetDirPath() string {
    envPath := os.Getenv("GO_CONFIG_DIR")
    if envPath == "" {
        envPath = "C:\\Users\\pamidu\\Desktop\\go"
    }
    envPath = filepath.Join(envPath,"config")
    return envPath
}
func loadConfig() Config{
    dirPath := GetDirPath()
    confPath := filepath.Join(dirPath, "default.json")
    content, operr := ioutil.ReadFile(confPath)
    if operr != nil {
        fmt.Println(operr)
    }
    defConfiguration := Config{}
    json.Unmarshal(content, &defConfiguration)
    envConfPath := filepath.Join(dirPath, "custom-environment-variables.json")
    envContent, operr := ioutil.ReadFile(envConfPath)
    if operr != nil {
        fmt.Println(operr)
    }else {
        defEnvConfiguration := Config{}
        unErr := json.Unmarshal(envContent, &defEnvConfiguration)
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
func createCategoryArray(host string,authToken string,tid string ,cid string) []string{
        url := fmt.Sprintf("http://%s/DVP/API/1.0.0.0/FileService/FileCategories",host)
        req, err := http.NewRequest("GET", url, nil)
        req.Header.Set("Authorization", authToken)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("companyinfo", tid+":"+cid)

        client := &http.Client{}
        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        defer resp.Body.Close()
        body, _ := ioutil.ReadAll(resp.Body)
        rep := Respondcatlist{}
        err = json.Unmarshal(body, &rep)
        var catlist []string
        index:=0
        for _, cat := range rep.Result {
           fmt.Println("press ", index+1, " for " ,cat.Category)
           catlist = append(catlist, cat.Category)
           index++
        }

        var catforPost []string
        fmt.Println("enter category list")
        fmt.Println("eg: 1,2,5")
        var categorylist string
        fmt.Scanln(&categorylist)
        s := strings.Split(categorylist, ",")
        for _, cat := range s {
           catint,_:=strconv.Atoi(cat)
           catforPost = append(catforPost, catlist[catint-1])
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
func getRecodes(url string,host string,authToken string,tid string,cid string,CatjsonStr []byte  )Respond{
        req, err := http.NewRequest("POST", url, bytes.NewBuffer(CatjsonStr))
        req.Header.Set("Authorization", authToken)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("companyinfo", tid+":"+cid)

        client := &http.Client{}
        resp, err := client.Do(req)
        checkErr(err)
        defer resp.Body.Close()
        body, _ := ioutil.ReadAll(resp.Body)
        rep := Respond{}
        err = json.Unmarshal(body, &rep)
        return rep
}
func fileWrite(rootPath string,rep Respond,confirm bool ,db mgo.Database){
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
                path := (rootPath+ "/"+"Company_"+strconv.Itoa(recods.CompanyId) + "_Tenant_" + strconv.Itoa(recods.TenantId) + "/" + datepath + "/")
                fmt.Println(path)
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
                   if(confirm){
                       status:= removeFile(db,recods.UniqueId)
                       if(status){
                        updatePath()
                       }
                   }
                }
                //execResponses <- true
            }(recods)

        }

        wg.Wait()

}
func removeFile(db mgo.Database,UniqueId string)bool{
    err := db.GridFS("fs").Remove(UniqueId)
    if(err == nil){
        fmt.Println("File Delete method :",UniqueId)
        return true
    }else{
        checkErr(err)
        return false
    }
    
}
func updatePath(){
    fmt.Println("File path update method :")
}
