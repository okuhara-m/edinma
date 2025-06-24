import requests
import sys
import datetime
import time
import json
from datetime import datetime, date, timedelta
from Arelle.arelle import Cntlr
import zipfile
import csv
import os
import shutil
import re
import glob
from html.parser import HTMLParser
#from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
#import chromedriver_binary
#from bs4 import BeautifulSoup

#RUNMODE
# 11:EDINETのインデックスファイルのダウンロード（期間指定）
# 21:EDINETのデータファイルダウンロード
# 22:ダウンロードファイルの存在確認
# 23:FYからCYへのデータ変換：fy2cy()
# 31(42):データファイルからの事業等のリスクの抽出 :extractrisk()
# 32(43):ZIPファイルからXBRLファイルの抽出 :zip2xbrl()
# 35:データファイルの中のキーワードカウント（ファイル出力付き,複数セクション対応）:readtext()
# 44:リスク情報テキストの１年分結合
# 45:リスク情報テキストのファイル名変更（証券コードから企業名へ）
# 46:リスク情報テキストの１年分結合（NAMED版）
# 51:XBRLファイルからパラメータの抽出 :readxbrl()
# 57:パラメータファイルの作成（基本情報のみ，INDEX不使用） :makeparamfile4()

#OBSOLATES
# 31(3):データファイルの中のキーワードカウント:readzip()
# 32(5):データファイルの中のキーワードカウント（ファイル出力付き）:countzip()
# 33:データファイルの中のキーワードカウント（ファイル出力付き）(NAMED版）:readtext()
# 34:データファイルの中のキーワードカウント（ファイル出力付き）(TEXT版）:readtext()
# 41(4):データファイルのテキスト化

#XBRLラベル　略号，英語名，qname, 集計種別
flabels = [
    ["NS" , "Net sales", "jpcrp_cor:NetSalesSummaryOfBusinessResults", "Duration"], #売上高
    ["OP" , "Ordinary profit (loss)", "jpcrp_cor:OrdinaryIncomeLossSummaryOfBusinessResults", "Duration"], # 経常利益又は経常損失（△）
    ["NA" , "Net assets", "jpcrp_cor:NetAssetsSummaryOfBusinessResults", "Instant"], #純資産額
    ["TA" , "Total assets", "jpcrp_cor:TotalAssetsSummaryOfBusinessResults", "Instant"], #総資産額
    ["ROE" , "Rate of return on equity", "jpcrp_cor:RateOfReturnOnEquitySummaryOfBusinessResults", "Duration"], #自己資本利益率
    ["EMP" , "Number of employees", "jpcrp_cor:NumberOfEmployees", "Instant"] #従業員数
]

plabels = [
    ["ROA1", "ROA of year 1"],
    ["ROA2", "ROA of year 2"],
    ["ROA3", "ROA of year 3"],
    ["PM", "Profit Margine"],
    ["ATO", "Asset Turn Over"],
    ["dPM", "Delta Profit Margine"],
    ["dATO", "Delta Asset Turn Over"],
    ["SC", "Security terms"]
]

RUNCOMMENT = "MODE 31（2020）EDI改版"

RUNMODE = 31
TRIAL = "C92"
MODULE = "ED1"
VERSION = "71"

D_START = "2020-01-01"
D_END = "2020-12-31" 
#D_START = "2023-10-01"
#D_END = "2023-12-31" 

IPOONLY = True
CONTENT = False
DEFLATE = False

deflator = {2017:1.005, 2018:1.004, 2019:1.012, 2020:1.019, 2021:1.018, 2022:1.027, 2023:1.0, 2024:1.0}

edinetlabels = {
    "【経営方針、経営環境及び対処すべき課題":"10", #21
    "【サステナビリティに関する考え方及び取":"10-2", #22
    "【事業等のリスク":"11", #23
    "【経営者による財政状態、経営成績及びキャッシュ・フローの状況の分析":"12", #24
    "【コーポレート・ガバナンスの概要":"35" #31
}

#RTYPES = [ "10013", "10014", "10017", "10264", "10372" ]
RTYPES = [ "21", "22", "23", "24", "31" ]
YEAR_RANGE = range(2020,2025) #rangeの仕様で最後の年は入らないことに注意

#for mode 52
FIGCAT = "EMP"
FIGTYPE = "1"

#for mode 53 / 54
OFFSET = 3
# 1-3の数値を取る．何年前の説明変数データを抽出するかを決める．

vlabels = [ "op0", "op1", "op2", "op3", "op4", "ta0", "ta1", "ta2", "ta3", "ta4", "ns3", "ns4"]

iyear = int(D_START[0:4])
#Finantial Yearを採用するときのコード
#if  D_START[5:7] in ("01", "02", "03"):
#    iyear = iyear -1

FYEAR = str(iyear)
HOMEDIR = "C:/Users/Master/Documents/IISEC/pyhome/edinet/"
HOMEDATA = HOMEDIR + "data/"
DATADIR = HOMEDIR + FYEAR + "/data/"
TEXTDIR = HOMEDIR + FYEAR + "/text/"
NAMEDDIR = HOMEDIR + FYEAR + "/named/"
XBRLDIR = HOMEDIR + FYEAR + "/xbrl/"
YEARDIR = HOMEDIR + FYEAR + "/"

#EDINETアクセスデータ
#2024-04-01変更
#url0 =  "https://disclosure.edinet-fsa.go.jp/api/v1/documents"
url0 =  "https://api.edinet-fsa.go.jp/api/v2/documents"
APIKEY = "** Put Your EDINET API Key Here **"

LOG_FILE =TRIAL + "_log.txt"

#WORDS = ["リスク","環境","社会的責任","感染","統制","コロナウイルス","コンプライアンス","コーポレート・ガバナンス","法律","インターネット","持続","セキュリティ","マネジメント","エネルギー","ネットワーク","デジタル","統治","持続可能","サイバー","プライバシー"]
WORDS = ["情報セキュリティ", "セキュリティ", "セキュリティ対策", "サイバーセキュリティ", "情報セキュリティマネジメントシステム", "セキュリティー", "情報セキュリティポリシー", "セキュリティインシデント", "ネットワーク・セキュリティ", "セキュリティポリシー" ]
#WORDS = ["セキュリティ"]

#INDEXファイル指定期間の省略時処理

if len(D_START) < 10:
    D_START += "-01"
    
if len(D_END) == 0:
    if D_START[5:7] == "02":
        endday = "-28"
    elif D_START[5:7] in ("04", "06", "09", "11"):
        endday = "-30"
    else:
        endday = "-31"

    D_END = D_START[0:7] + endday

D_RANGE = D_START[0:4] + D_START[5:7] + D_START[8:10] + "_" + D_END[0:4] + D_END[5:7] + D_END[8:10]
#D_RANGE = "20230421_20230430"

IDX_FILE = MODULE + "_" + D_RANGE + ".csv"


#----------------------------------------------------------------

class Parser(HTMLParser):

    # self.text は {h3セクションタイトル : 本文} のdictionaryペア
    
    def __init__(self):
        HTMLParser.__init__(self)
        self.title = False
        self.target = False
        self.text = {}
        self.titleid = "" #初期値は必要ないが念のため
        self.warnings = ""
        self.titletext = ""

    def handle_starttag(self, tag, attrs):

        if tag[0] == "h":
            self.title = True
            self.titletext = ""

    def handle_endtag(self, tag):

        self.newsection = False

        if tag.startswith("body"): 
            self.target = False # 本文終了
                
        if self.title == True and tag[0] == "h":
    
            self.title = False
            needle1 = self.titletext.find("【")
            needle2 = self.titletext.find("】")

            if self.target == True:

#                if self.titleid == "35":
#                    if tag[0:1] in ["h4", "h3", "h2" ,"h1"] :
#                        self.target = False # 本文終了
#                    else:
#                        self.text[self.titleid] += self.titletext
#                else:

                if needle1 > 0:
                    self.target = False # 何であれセクションが変わっているので一旦本文は終了
                    self.newsection = True
                else:
                    self.text[self.titleid] += self.titletext
    
            elif needle1 > 0:
                self.newsection = True

            if self.newsection == True:

                self.titlelabel = self.titletext[needle1:needle2+1] #【】の部分のみ抽出
                self.titlelabel = self.titlelabel.replace("，","、") # 全角カンマの正規化
                
                for l in edinetlabels.keys():
                    
                    if self.titlelabel.startswith(l):
                        self.target = True
                        self.titleid = edinetlabels[l]
                        self.text[self.titleid] = "" #self.titletext

                        if self.titleid == "35":
                            if tag[1] != "4":
                                self.warnings += (" HEADER LEVEL != 4:" + tag + ":" + self.titlelabel + "\n")
                        else:
                            if tag[1] != "3":
                                self.warnings += (" HEADER LEVEL != 3:" + tag + ":" + self.titlelabel + "\n")


    def handle_data(self, data):

        if self.title == True:
            self.titletext += data

        elif self.target == True:
            self.text[self.titleid] += data


#----------------------------------------------------------------

class HChecker(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.title = False
        self.text = [] #戻り値
        self.titletext = ["","","","",""] #テキストpathの格納エリア
        self.textintag = ""

    def handle_starttag(self, tag, attrs):
        if tag in ("h1", "h2", "h3", "h4"):
            self.title = True
            self.level = int(tag[1])
            self.textintag = ""

    def handle_endtag(self, tag):
        if tag in ("h1", "h2", "h3", "h4"):
            self.title = False

            needle1 = self.textintag.find("【")
            needle2 = self.textintag.find("】")

            if needle1 > 0:

                self.textintag = self.textintag[needle1:needle2+1] #【】の部分のみ抽出
            
                for i in range(self.level, 5): #自分より下のレベルのヘッダをリセット
                    self.titletext[i] = ""
    
                self.titletext[self.level] = self.textintag #自分のレベルを記録
    
                titlepath = ""
                for i in range(self.level): #ヘッダパスを再構築
                    titlepath += ("/" + self.titletext[i+1])

                self.text.append(titlepath)

    def handle_data(self, data):

        if self.title == True:
            self.textintag += data

#----------------------------------------------------------------

class HChecker2(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.title = False
        self.text = [] #戻り値
        self.titletext = ["","","","",""] #テキストpathの格納エリア
        self.textintag = ""

    def handle_starttag(self, tag, attrs):
        if tag in ("h1", "h2", "h3", "h4"):
            self.title = True
            self.level = int(tag[1])
            self.textintag = ""

    def handle_endtag(self, tag):
        if tag in ("h1", "h2", "h3", "h4"):
            self.title = False

            for i in range(self.level, 5): #自分より下のレベルのヘッダをリセット
                self.titletext[i] = ""

            self.titletext[self.level] = self.textintag #自分のレベルを記録

            titlepath = ""
            for i in range(self.level): #ヘッダパスを再構築
                titlepath += ("/" + self.titletext[i+1])

                
                           

            self.text.append(titlepath)

    def handle_data(self, data):

        if self.title == True:
            self.textintag += data

#----------------------------------------------------------------

def out(fp, strin):
#    print(strin)
    fp.write(strin)

    return 0


#----------------------------------------------------------------

def logwrite(fp, strin, cont=False):

    print(strin)

    if cont:
        fp.write(strin + ":")
    else:
        fp.write(strin + "\n")

    return 0

#----------------------------------------------------------------

def getindex(fp_log, fp_idx, dates):
    
    global url0
 
    for i in range(10):
        print(".", end="")
        time.sleep(1)

    print(dates, end = ":")
    
    parms = {'date': dates, 'type' : '2', 'Subscription-Key' : APIKEY}
    url = url0 + ".json"
    res = requests.get(url, params=parms)
    res.encoding = res.apparent_encoding

    #    print(res.text)
    
    jdata = res.json()
    cnt = jdata["metadata"]["resultset"]["count"]
#    print("Return Code:" + jdata["metadata"]["status"])
    print("Total:" + str(cnt), end = "")

    nor = 0

    for rs in jdata["results"]:
        dtcode = str(rs["docTypeCode"])
        ccode = str(rs["secCode"])
        if dtcode == "120":
            nor += 1
            out(fp_idx, dates + ",")
            out(fp_idx, str(rs["edinetCode"]) + ",")
            out(fp_idx, ccode[0:4] + ",")
            out(fp_idx, dtcode + ",")
            out(fp_idx, str(rs["docID"]) + "\n")
        
    print(" / Extracted:" + str(nor))

    return nor


#----------------------------------------------------------------


def getfile(fp_log, id):

    global url0
   
    for i in range(10):
        print(".", end="")
        time.sleep(1)
    
    parms = {'type' : '1', 'Subscription-Key' : APIKEY}
    url = url0 + "/" + str(id)
#    print("[" + url + "]")
    print(" calling...", end="")
    res = requests.get(url, params=parms)
    res.encoding = res.apparent_encoding

#    print(res.text)
    
    if res.headers['Content-Type'] != "application/octet-stream":
        print("Content type error:" + res.headers['Content-Type'])
        print(res.text)
        return -1

    ids = str(id)
    zfile = DATADIR + ids +".zip"
    
    with open(zfile, "wb") as fp_zip:
        fp_zip.write(res.content)

    print("received.")
        
    return 0

#----------------------------------------------------------------

def checkfile(fp_log, id):

    global url0
   
    ids = str(id)
    zfile = DATADIR + ids +".zip"

    if(os.path.isfile(zfile) == False):
        print("File missed:" + ids)
#        fp_log.write("File missed:" + ids + "\n")

    return 0

#----------------------------------------------------------------

def zip2xbrl(fp_log, fp_list, id, ccode):

    ids = str(id)
    zfile = DATADIR + ids +".zip"
    with zipfile.ZipFile(zfile) as fp_zip:
    
        for cf in fp_zip.namelist():

            if (cf.startswith("XBRL/PublicDoc/jpcrp030000")) and cf.endswith("xbrl"):

                rv = fp_zip.extract(cf, path = XBRLDIR)
                fp_list.write(ids + "," + ccode + "," + rv + "\n")

            elif (cf.startswith("XBRL/PublicDoc/jpcrp030000")) and cf.endswith("xsd"):

                rv = fp_zip.extract(cf, path = XBRLDIR)

            elif (cf.startswith("XBRL/PublicDoc/jpcrp030000")) and cf.endswith("xml"):

                rv = fp_zip.extract(cf, path = XBRLDIR)

    return 0

#----------------------------------------------------------------

def readxbrl(fp_log, ccode, cf):

    ctrl = Cntlr.Cntlr(logFileName=None)
    model_xbrl = ctrl.modelManager.load(cf)

    with open(XBRLDIR + ccode + ".csv" ,"w") as fp_out:
        
        ctrl = Cntlr.Cntlr(logFileName='logToPrint')
        model_xbrl = ctrl.modelManager.load(cf)

        for fact in model_xbrl.facts:
    
            # 必要情報の取得
            label_ja = fact.concept.label(preferredLabel=None, lang='ja', linkroleHint=None)
            label_en = fact.concept.label(preferredLabel=None, lang='en', linkroleHint=None)
            id = fact.contextID
            qname = fact.qname
            try:
               # 数値
               value = fact.vEqValue
            except ValueError as e:
                pass

            qname = str(qname)
            
            for i in range(len(flabels)):

                if qname == flabels[i][2]:
                    
                    if id.startswith("CurrentYear"):
                        col1 = 1
                    elif id.startswith("Prior1Year"):
                        col1 = 2
                    elif id.startswith("Prior2Year"):
                        col1 = 3
                    elif id.startswith("Prior3Year"):
                        col1 = 4
                    elif id.startswith("Prior4Year"):
                        col1 = 5
                    elif id.startswith("Prior5Year"):
                        col1 = 6
                    else:
                        col1 = 0

                    if id.endswith("NonConsolidatedMember"):
                        col2 = 1
                    else:
                        col2 = 0
                    
                    fp_out.write(flabels[i][0] + "," + str(col1) + "," + str(col2) + "," + str(value) + "," + id + "\n") 
        
        print(".", end = "")

    return 0

#----------------------------------------------------------------

def makeparamfile4(fp_log, fp_out, ccode, dyear):

    val = {}
    defr = {}

    plabels = ["op", "ta", "ns", "emp"]
    
    if DEFLATE:
        defr = deflator[dyear - iy]
    else:
        defr = 1.0

    iy = 0
    with open(XBRLDIR + ccode + ".csv" ,"r") as fp_in:

        csvr = csv.reader(fp_in)

        # 読み込み
        
        for rec in csvr:
            if len(rec) < 4:
                break

            rcat = rec[0]
            ryear = str(rec[1])
            rtype = str(rec[2])
            value = rec[3]
            xbrlid = rec[4]

            if rtype == FIGTYPE:

                if ryear == "1":

                    if rcat == "OP":
                        val["op"] = defr * float(value)

                    elif rcat == "TA":
                        val["ta"] = defr * float(value)

                    elif rcat == "NS":
                        val["ns"] = defr * float(value)

                    elif rcat == "EMP":
                        val["emp"] = defr * float(value)

        # 確認
        
        for ix in plabels:
            
            if ix not in val:
                errmsg = "Warning: value missing:" + ix + " in " + ccode
                print(errmsg)
                fp_log.write(errmsg + "\n")
                return 1
                
            elif val[ix] == 0:
                errmsg = "Warning: parameter equal to 0 : " + ix + " in " + ccode
                print(errmsg)
                fp_log.write(errmsg + "\n")
                return 1

        # 計算
        
        roa = val["op"] / val["ta"] # ROA
        pm = val["op"] / val["ns"]  # Operational Profit
        ato = val["ns"] / val ["ta"] # Asset Turn Over

        # 書き出し
        # outbuf = "ID,CCODE,YEAR,ROA,PM,ATO,dROA,dPM,dATO,OP,TA,NS,EMP\n"

        outbuf = str(ccode) + "_" + str(dyear-iy) + "," + str(ccode) + "," + str(dyear-iy) 
        outbuf = outbuf + "," + str(roa)
        outbuf = outbuf + "," + str(pm)
        outbuf = outbuf + "," + str(ato)
       
        for ix in  plabels:
            outbuf = outbuf + "," + str(val[ix])
        
        fp_out.write(outbuf + "\n")
                
    return 0

#----------------------------------------------------------------

def extractrisk(fp_log, fp_list, id, pdate, ccode, eid):
    
    ids = str(id)
    zfile = DATADIR + ids +".zip"


    p1 = re.compile(r"<[^>]*?>") #タグ削除
    p2 = re.compile('&#.*;') #代替文字削除
    
    with zipfile.ZipFile(zfile) as fp_zip:

        f_text = ""

        for cf in fp_zip.namelist():

            if (cf.startswith("PublicDoc") or cf.startswith("XBRL/PublicDoc")) and not cf.endswith("/"):
                
                with fp_zip.open(cf) as fp_rep:

                    if not cf.endswith("htm"):
                        break

                    in_file = False
                    in_section = False
                    t_text = ""
                    
                    if "honbun" in cf:

                        b_rec = fp_rep.readline()

                        b_headline = b_rec.decode("utf-8")
                        b_headline = b_headline.encode("cp932", "ignore").decode("cp932") #変換できないコード除け（ex. 0xae)

                        # b'<?xml version="1.0" encoding="utf-8"?>\r\n'
                        needle1 = b_headline.find("encoding=")
                        if needle1 < 0:
                            print() #改行
                            logwrite(fp_log, "Header Error 1 in " + id + ":" + b_headline)
                            break
                        
                        needle2 = b_headline.find('"', needle1)
                        if needle2 < 0:
                            print() #改行
                            logwrite(fp_log, "Header Error 2 in " + id + ":" + b_headline)
                            print(b_headline)
                            break

                        needle3 = b_headline.find('"', needle2 + 1)
                        if needle2 < 0:
                            print() #改行
                            logwrite(fp_log, "Header Error 3 in " + id + ":" + b_headline)
                            break

                        encoding = b_headline[needle2 + 1 : needle3]
                        #print(str(needle2) + "-" + str(needle3) + "[" + encoding + "]")

                        if encoding == "UTF-8":
                            encoding = "utf-8"
                            
                        if encoding != "utf-8":
                            print() #改行
                            logwrite(fp_log, "WARTING: encode is not utf-8: " + encoding + " in " + id)

                        while(True):
                            try:
                                b_text = b_rec.decode(encoding)
                            except UnicodeDecodeError:
                                logwrite(fp_log, "decode failed:" + cf)
                                break

                            t_text += b_text
                            b_rec = fp_rep.readline()

                            if(not b_rec):
                                break

                        f_text += t_text
                        
    parser = Parser()
    parser.feed(f_text)
    parser.close()

    if len(parser.warnings) > 0:
        print() #改行
        logwrite(fp_log, "WARNING IN PARSER: ID=" + id + " CODE=" + ccode)
        logwrite(fp_log, parser.warnings)

    for l in edinetlabels.values():

        if l in parser.text:

            fp_list.write(pdate + "," + id + "," + ccode + "," + l + "," + cf + "\n")
            fp_out = open(TEXTDIR + l + "/" + ccode + "_" + FYEAR + ".txt","w", encoding="utf-8")
#                                fp_out = open(TEXTDIR + sec + "/" + ccode + "_" + eid + "_" + FYEAR + ".txt","w", encoding="utf-8")
#                                print(TEXTDIR + sec + "/" + ccode + "_" + FYEAR + ".txt")

#                                    lw = b_text.encode("cp932", "ignore").decode("cp932") #変換できないコード除け（ex. 0xae)
            lw = parser.text[l]
            lw = p1.sub('', lw) #タグ削除
            lw = p2.sub('', lw) #代替文字削除
#                                if not lw.isascii(): #英字だけの行は削除
            fp_out.write(lw)
            fp_out.close()
    

#----------------------------------------------------------------

def headercheck(fp_log, id, pdate, ccode, eid):
    
    ids = str(id)
    zfile = DATADIR + ids +".zip"
    headerlist = []

    with zipfile.ZipFile(zfile) as fp_zip:

        f_text=""
        
        for cf in fp_zip.namelist():

#            print(cf)
            
            if (cf.startswith("PublicDoc") or cf.startswith("XBRL/PublicDoc")) and not cf.endswith("/"):
                
                with fp_zip.open(cf) as fp_rep:

                    if not cf.endswith("htm"):
                        break

                    filecode = cf[0:6]
                    if filecode == "0000000": #表紙ファイルを除外
                        break
                    
                    in_file = False
                    in_section = False
                    t_text = ""

                    b_rec = fp_rep.readline()

                    b_headline = b_rec.decode("utf-8")
                    b_headline = b_headline.encode("cp932", "ignore").decode("cp932") #変換できないコード除け（ex. 0xae)

                    # b'<?xml version="1.0" encoding="utf-8"?>\r\n'
                    needle1 = b_headline.find("encoding=")
                    if needle1 < 0:
                        logwrite(fp_log, "Header Error 1 in " + id + ":" + b_headline)
                        break
                    
                    needle2 = b_headline.find('"', needle1)
                    if needle2 < 0:
                        logwrite(fp_log, "Header Error 2 in " + id + ":" + b_headline)
                        print(b_headline)
                        break

                    needle3 = b_headline.find('"', needle2 + 1)
                    if needle2 < 0:
                        logwrite(fp_log, "Header Error 3 in " + id + ":" + b_headline)
                        break

                    encoding = b_headline[needle2 + 1 : needle3]
                    #print(str(needle2) + "-" + str(needle3) + "[" + encoding + "]")

                    if encoding == "UTF-8":
                        encoding = "utf-8"
                        
                    if encoding != "utf-8":
                        logwrite(fp_log, "WARTING: encode is not utf-8: " + encoding + " in " + id)

                    while(True):
                        try:
                            b_text = b_rec.decode(encoding)
                        except UnicodeDecodeError:
                            logwrite(fp_log, "decode failed:" + cf)
                            break

                        t_text += b_text
                        b_rec = fp_rep.readline()

                        if(not b_rec):
                            break

                    f_text += t_text
                            
    parser = HChecker()
    parser.feed(f_text)
    parser.close()

    headerlist += parser.text

#                        for l in parser.text:
#                            print(l)

#空白削除
    for i,n in enumerate(headerlist):
#        headerlist[i] = n.replace("\n","").replace("\r","")
        headerlist[i] = "".join(n.replace("\n","").replace("\r","").replace(",","[COMMA]").split())

    return(headerlist)


#----------------------------------------------------------------

def mergerisktext(fp_log, fp_out, ccode):

    infile = TEXTDIR + ccode + "_" + FYEAR + ".txt"
    with open(infile,"r", encoding="utf_8_sig") as fp_in:

        for line in fp_in:

#            outbuf = line.encode("cp932", "ignore").decode("cp932") #変換できないコード除け（ex. 0xae)

            fp_out.write(line)

    return 0

#----------------------------------------------------------------

def readtext(fp_log, fp_out, fn, ccode, source):
    
    wc = {}
    tc = 0

    filepath = source + str(fn)
#    print(".", end="")
    with open(filepath,"r", encoding="utf_8_sig") as fp_rep:

        for key in WORDS:
            wc[key] = 0

        while(True):

            try:
                b_text = fp_rep.readline()
            except UnicodeDecodeError:
                logwrite(fp_log, "Decode Error in " + filepath)
                break

            if(not b_text):
                break

            b_text = b_text.replace(" ","")
            b_text = b_text.replace("\n","")
            b_text = b_text.replace("\t","")
            tc += len(b_text)
            
            for key in WORDS:

                kcnt =  b_text.count(key)
                if kcnt > 0:
                    wc[key] += 1

    outbuf = ccode
    
    for key in WORDS:
        outbuf += ("," + str(wc[key]))

    outbuf += ("," + str(tc))
    outbuf += "\n"

    fp_out.write(outbuf)

    return 0

#----------------------------------------------------------------

def moveifexist(fp_log, p_from, p_to):

    if(os.path.isfile(p_from) == False):
        logwrite(fp_log, "File missed:" + p_from, cont=False)
        return -1

    shutil.move(p_from, p_to) 
    print("moving: " + p_from + " to " + p_to)
    return 0

#----------------------------------------------------------------

def fin2cal(fp_log, id, pyear, pmonth, ccode):

    #例：2020-01-01 のデータを 2019 から 2020 に移動する
    iyear_to = int(pyear)
    iyear_from = iyear_to - 1

    pyear_to = str(iyear_to)
    pyear_from = str(iyear_from)

    moveifexist(fp_log, HOMEDIR + pyear_from + "/data/" + id + ".zip", HOMEDIR + pyear_to + "/data/" + id + ".zip")
    moveifexist(fp_log, HOMEDIR + pyear_from + "/text/" + ccode + "_" + pyear_from + ".txt", HOMEDIR + pyear_to + "/text/" + ccode + "_" + pyear_to + ".txt",)
    moveifexist(fp_log, HOMEDIR + pyear_from + "/xbrl/" + ccode + ".csv", HOMEDIR + pyear_to + "/xbrl/" + ccode + ".csv")

    return 0


#----------------------------------------------------------------
# main

print("Start:" + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

with open(HOMEDIR + LOG_FILE, 'a', encoding="utf-8") as fp_log: 
    
    called = 0
    
    fp_log.write("================\n")
    fp_log.write("START:" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n")
    fp_log.write("MODE:" + str(RUNMODE) + " / ")
    fp_log.write("MODULE:" + MODULE + " / ")
    fp_log.write("VERSION:" + VERSION + " / ")
    fp_log.write("TRIAL:" + TRIAL + " / ")
    fp_log.write("IPOONLY:" + str(IPOONLY) + "\n")
    fp_log.write("D_START:" + D_START + "\n")
    fp_log.write("D_END:" + D_END + "\n")
    fp_log.write("COMMENT:" + RUNCOMMENT + "\n")

    # get edinet index file
    if RUNMODE == 11:

        dstart = datetime.strptime(D_START, '%Y-%m-%d')
        dend = datetime.strptime(D_END, '%Y-%m-%d')
        dnow = dstart
        
        idxfile = HOMEDIR + MODULE  + "_" + datetime.strftime(dstart, '%Y%m%d') + "_" + datetime.strftime(dend, '%Y%m%d') + ".csv"


        with open(idxfile, "w") as fp_idx: 

            while dnow <= dend:

                d_now = datetime.strftime(dnow, '%Y-%m-%d')
#                print(d_now)
                rv = getindex(fp_log, fp_idx, d_now)
                called += 1
                fp_log.write("- " + d_now + ": extracted:" + str(rv) + "\n")

                dnow += timedelta(days=1)

        print("Next step:21")

    # get edinet zip file
    elif RUNMODE == 21 or RUNMODE == 22:
        
        count = 0
        received = 0
        tcount = 0

        with open(HOMEDIR + IDX_FILE, "r") as fp_idx: 
            for line in fp_idx:
                tcount += 1

        
        with open(HOMEDIR + IDX_FILE, "r") as fp_idx: 
            
            csvr = csv.reader(fp_idx)

            for rec in csvr:
                
                count += 1
                
                if len(rec) < 4:
                    break
                    
                id = rec[4]

                if RUNMODE == 21:
                    print(str(count) + "/" + str(tcount), id, end=":")

                if IPOONLY and rec[2] == "None":
                    if RUNMODE == 21:
                        print("No Sec Code.")

                elif len(str(id)) > 0:
                    if RUNMODE == 21:
                        rv = getfile(fp_log, id)
                        if rv < 0:
                            break
                    else:
                        rv = checkfile(fp_log, id)

                    called += 1
                    received += 1

        print("Called:" + str(called))

        fp_log.write("TARGET DIRECTORY:" + DATADIR + "\n")
        fp_log.write("RECEIVED:" + str(received) + "\n")

        print("Next step:43 before INDEX concatenation by shell.txt")

    
    elif RUNMODE == 23:

        with open(HOMEDIR + IDX_FILE, "r") as fp_idx: 

            csvr = csv.reader(fp_idx)

            for rec in csvr:
                if len(rec) < 4:
                    break

                id = rec[4]
                pdate = rec[0]
                ccode = rec[2]
                pyear = pdate[0:4]
                pmonth = pdate[5:7]

                if IPOONLY and rec[2] == "None":
                    print(id + ": No Sec Code.")
                else:
                    if pmonth in ["01", "02", "03"]:
                        print(id)
                        rv = fin2cal(fp_log, id, pyear, pmonth, ccode)
                        print(id + " month = " + pmonth + ":processed") 
                        called += 1
                    else:
                        print(id + " month = " + pmonth + ":skipped") 
    
    elif RUNMODE == 31 or RUNMODE == 32:

        called = 0
        
        if RUNMODE == 31:
            outfile = "L_RISKTEXT_"
        else:
            outfile = "L_XBRLZIP_"

        # サブディレクトリの準備
        if RUNMODE == 31:
            for tn in edinetlabels.values():
                target = TEXTDIR + str(tn)
                if not os.path.isdir(target):
                    os.makedirs(target)
        
        with open(HOMEDIR + IDX_FILE, "r") as fp_idx: 
            
            with open(HOMEDIR + outfile + FYEAR + ".csv", "w") as fp_list: 

                csvr = csv.reader(fp_idx)
    
                for rec in csvr:
                    if len(rec) < 4:
                        break
    
    
                    id = rec[4]
                    pdate = rec[0]
                    eid = rec[1]
                    ccode = rec[2]
    
                    if IPOONLY and rec[2] == "None":
#                        print(id + ": No Sec Code.")
#                        print(".", end = "")
                        pass
                    else:
#                        print("*", end = "")
                        if RUNMODE == 31:
                            rv = extractrisk(fp_log, fp_list, id, pdate, ccode, eid)
                        else:
                            rv = zip2xbrl(fp_log, fp_list, id, ccode)

                    called += 1
                    if called % 100 == 0:
                        print("[" + str(called) + "]", end = "")
                        
        if RUNMODE == 31:
            print("Next step:35")
        else:
            print("Next step:51")
                       
    elif RUNMODE ==35:
    
        fp_log.write("KEYWORDS:")
        for key in WORDS:
            fp_log.write("[" + key + "]")
        fp_log.write("\n")

        for tyear in YEAR_RANGE:

            fyear = str(tyear)

            for rtype in RTYPES:
            
                called = 0
                logwrite(fp_log, "YEAR=" + fyear + ": TYPE=" + rtype, cont=True)

                source = HOMEDIR + fyear + "/text/" + rtype + "/"
                print("Reading from: [" + source + "]", end=" ")

                if not os.path.isdir(source):
                    logwrite(fp_log, " SKIPPED" + str(called))
                    continue

                files = os.listdir(source)
                if len(files) > 0:
                    
                    with open(HOMEDATA + MODULE + "_" + TRIAL + "_" + fyear + "_" + rtype + "_out.txt","w", encoding="utf-8") as fp_out:

                        #ヘッダ
                        outbuf = "CCODE"
                        for key in WORDS:
                            outbuf += ","
                            outbuf += key
                        outbuf += ",COUNT\n"
                        fp_out.write(outbuf)

                        #データ
                        for fn in files:
                            ccode = fn[0:4]
            
                            rv = readtext(fp_log, fp_out, fn, ccode, source)
                            called += 1

                            if called % 100 == 0:
                                print("[" + str(called) + "]", end = "")

                logwrite(fp_log, " FILES=" + str(called))

    elif RUNMODE == 44:
        
        with open(HOMEDIR + "L_RISKTEXT_" + FYEAR + ".csv", "r") as fp_idx: 
            
            with open(YEARDIR + "RISKTEXT.txt", "w", encoding="utf-8") as fp_out: 

                csvr = csv.reader(fp_idx)
    
                for rec in csvr:
                    if len(rec) < 1:
                        break
    
                    id = rec[1]
                    ccode = rec[2]
    
                    rv = mergerisktext(fp_log, fp_out, ccode)
                    called += 1
                
    elif RUNMODE == 45:

        keyerr = 0
        codedict = {}
        with open(HOMEDIR + "data_j_codelist.csv", "r", encoding="utf_8_sig") as fp_idx: 
            csvd = csv.DictReader(fp_idx)

            for rec in csvd:
                codedict[rec["SCODE"]] = rec["NAME"]

        files = os.listdir(TEXTDIR)
        for fn in files:
            scode = fn[0:4]
            try:
                cname = codedict[scode]
            except KeyError:
                print("Key Error:" + scode)
                fp_log.write("Key Error:" + scode + "\n")
                keyerr += 1
                continue

#            shutil.copy(TEXTDIR + fn, NAMEDDIR + cname)
            shutil.copy(TEXTDIR + fn, NAMEDDIR + scode)
            called += 1

        print("Key Errors:" + str(keyerr))
            
    elif RUNMODE == 46:

        with open(YEARDIR + "NAMEDTEXT.txt", "a", encoding="utf-8") as fp_out: 
         
            files = os.listdir(NAMEDDIR)
            print(NAMEDDIR)
            for fn in files:

                with open(NAMEDDIR + fn,"r", encoding="utf_8_sig") as fp_in:

                    for line in fp_in:
                        fp_out.write(line)

                called += 1
               
              
    elif RUNMODE == 47:

        outfile = "HEADER_PROOF_"
        dmpfile = "HEADER_DUMP_"
        headerlist = {}
        
        with open(HOMEDIR + IDX_FILE, "r") as fp_idx: 
            
            with open(HOMEDIR + dmpfile + FYEAR + ".csv", "w", encoding="utf-8") as fp_dmp: 
                with open(HOMEDIR + outfile + FYEAR + ".csv", "w", encoding="utf-8") as fp_out: 

                    csvr = csv.reader(fp_idx)
        
                    for rec in csvr:
                        if len(rec) < 4:
                            break
        
                        id = rec[4]
                        pdate = rec[0]
                        eid = rec[1]
                        ccode = rec[2]
        
                        if IPOONLY and rec[2] == "None":
                            print(".", end = "")
    
                        else:
                            print("*", end = "")
                            text = headercheck(fp_log, id, pdate, ccode, eid)
    
                            for l in text:
                                fp_dmp.write(id + "," + eid + "," + l + "\n")

                                if l in headerlist:
                                    headerlist[l] += 1
                                else:
                                    headerlist[l] = 1
                            called += 1
    
                    for l in headerlist:
                        fp_out.write(l + "," + str(headerlist[l]) + "\n")
    
    
    elif RUNMODE == 51:

        print("Caution: This proccess takes several hours.")
        print("         Network connectivity required to decode xbrl.")
        with open(HOMEDIR + "L_XBRLZIP_" + FYEAR + ".csv", "r") as fp_idx: 
            
            csvr = csv.reader(fp_idx)

            for rec in csvr:
                if len(rec) < 1:
                    break

                id = rec[0]
                ccode = rec[1]
                cf = rec[2]

                rv = readxbrl(fp_log, ccode, cf)
                called += 1
                
    elif RUNMODE == 57:

        dyear = int(FYEAR)

        files = os.listdir(XBRLDIR)
        
        warnings = 0

        with open(YEARDIR +  "FIGURES_" + str(FYEAR)  + ".csv", "w", newline='') as fp_out: 

            outbuf = "ID,CCODE,YEAR,ROA,PM,ATO,OP,TA,NS,EMP\n"
            fp_out.write(outbuf)

            for fn in files:

                if fn.endswith('csv'):
                    ccode = fn[0:4]
        
                    rv = makeparamfile4(fp_log, fp_out, ccode, dyear)
                    called += 1
    
                    if rv > 0:
                        warnings += 1
        
            print(str(called) + " processed.")
            print(str(warnings) + " warnings.")
            fp_log.write("WARNINGS:" + str(warnings) + "\n")

    
    else:

        print("undefined run mode:" + str(RUNMODE))

    fp_log.write("PROCESSED:" + str(called) + "\n")
    fp_log.write("END:" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n\n")

print("Done:" + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

