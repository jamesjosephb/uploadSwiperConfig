import csv
import config
import re




from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Sequence
engine = create_engine(config.mySQLinfo)
Base = declarative_base()


class SwiperConfig(Base):
    __tablename__ = 'SwiperConfigs'
    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    SiteID = Column(String(12))
    Version = Column(Integer)
    Swiper_MAC_address = Column(String(16))
    SwiperProfile = Column(String(12))
    MinCh = Column(Integer)
    MaxCh = Column(Integer)
    CoinVal = Column(Integer)
    Rate = Column(Integer)
    BnsCoin = Column(Integer)
    SigType = Column(String(8))
    Cancel = Column(String(3))
    Amex = Column(String(3))
    MustApp = Column(String(3))
    Fleet = Column(String(3))
    Debug = Column(String(3))
    BtnAdd = Column(String(3))
    AppDly = Column(Integer)
    PrchTyp = Column(String(10))
    SwiperName = Column(String(8))
    CoinTim = Column(Integer)
    ICTime = Column(Integer)
    AppTime = Column(Integer)
    BnsMode = Column(String(10))
    BnsCnt = Column(Integer)
    BnsTime = Column(Integer)
    PrchLim = Column(Integer)

    def setSwiperConfigfromList(self, swiperConfigList = []):
        self.SiteID = swiperConfigList[0]
        self.Version = swiperConfigList[1]
        self.Swiper_MAC_address = swiperConfigList[2]
        self.SwiperProfile = swiperConfigList[3]
        self.MinCh = swiperConfigList[4]
        self.MaxCh = swiperConfigList[5]
        self.CoinVal = swiperConfigList[6]
        self.Rate = swiperConfigList[7]
        self.BnsCoin = swiperConfigList[8]
        self.SigType = swiperConfigList[9]
        self.Cancel = swiperConfigList[10]
        self.Amex = swiperConfigList[11]
        self.MustApp = swiperConfigList[12]
        self.Fleet = swiperConfigList[13]
        self.Debug = swiperConfigList[14]
        self.BtnAdd = swiperConfigList[15]
        self.AppDly = swiperConfigList[16]
        self.PrchTyp = swiperConfigList[17]
        self.SwiperName = swiperConfigList[18]
        self.CoinTim = swiperConfigList[19]
        self.ICTime = swiperConfigList[20]
        self.AppTime = swiperConfigList[21]
        self.BnsMode = swiperConfigList[22]
        self.BnsCnt = swiperConfigList[23]
        self.BnsTime = swiperConfigList[24]
        self.PrchLim = swiperConfigList[25]

    def printswiperconfig(self):
        #print("SiteID\t\tVersion\t\tSwiper_MAC_address\t\tSwiperProfile\t\tMinCh\t\tMaxCh\t\tCoinVal\t\tRate\t\tBnsCoin\t\tSigType\t\tCancel\t\tAmex\t\tMustApp\t\tFleet\t\tDebug\t\tBtnAdd\t\tAppDly\t\tPrchTyp\t\tSwiperName\t\tCoinTim\t\tICTime\t\tAppTime\t\tBnsMode\t\tBnsCnt\t\tBnsTime\t\tPrchLim")
        print(self.SiteID, ",",
        self.Version, ",",
        self.Swiper_MAC_address,",",
        self.SwiperProfile,",",
        self.MinCh,",",
        self.MaxCh,",",
        self.CoinVal,",",
        self.Rate,",",
        self.BnsCoin,",",
        self.SigType,",",
        self.Cancel,",",
        self.Amex,",",
        self.MustApp,",",
        self.Fleet,",",
        self.Debug,",",
        self.BtnAdd,",",
        self.AppDly,",",
        self.PrchTyp,",",
        self.SwiperName,",",
        self.CoinTim,",",
        self.ICTime,",",
        self.AppTime,",",
        self.BnsMode,",",
        self.BnsCnt,",",
        self.BnsTime,",",
        self.PrchLim
              )


    def getswiperconfigCSV(self):
        #print("SiteID\t\tVersion\t\tSwiper_MAC_address\t\tSwiperProfile\t\tMinCh\t\tMaxCh\t\tCoinVal\t\tRate\t\tBnsCoin\t\tSigType\t\tCancel\t\tAmex\t\tMustApp\t\tFleet\t\tDebug\t\tBtnAdd\t\tAppDly\t\tPrchTyp\t\tSwiperName\t\tCoinTim\t\tICTime\t\tAppTime\t\tBnsMode\t\tBnsCnt\t\tBnsTime\t\tPrchLim")
        CSV = (self.SiteID,
        self.Version,
        self.Swiper_MAC_address,
        self.SwiperProfile,
        self.MinCh,
        self.MaxCh,
        self.CoinVal,
        self.Rate,
        self.BnsCoin,
        self.SigType,
        self.Cancel,
        self.Amex,
        self.MustApp,
        self.Fleet,
        self.Debug,
        self.BtnAdd,
        self.AppDly,
        self.PrchTyp,
        self.SwiperName,
        self.CoinTim,
        self.ICTime,
        self.AppTime,
        self.BnsMode,
        self.BnsCnt,
        self.BnsTime,
        self.PrchLim
              )
        return str(CSV)


#reference https://docs.python.org/3/library/codecs.html#module-codecs
def readCSV(CSV, **options):
    if CSV.endswith(".csv"):
        if options.get("action") == "WriteCSV":
            editedconfig = open("editedconfig.csv", "w+")
            editedconfig.write("SiteID,Version,Swiper_MAC_address,SwiperProfile,MinCh,MaxCh,CoinVal,Rate,BnsCoin,SigType,Cancel,Amex,MustApp,Fleet,Debug,BtnAdd,AppDly,PrchTyp,SwiperName,CoinTim,ICTime,AppTime,BnsMode,BnsCnt,BnsTime,PrchLim\n")
        elif options.get("action") == "WriteSQL":
            # Checks if table exists, if it does not it will create any table from a class that inherits Base.
            Base.metadata.create_all(engine)
            Session = sessionmaker(bind=engine)
        with open(CSV, encoding="latin-1") as csv_file:
            csv_reader = csv.reader(csv_file)
            incompleteRows = []
            line = 0

            #The two below lines skip the first row in the for loop below
            itercsv_reader = iter(csv_reader)
            next(itercsv_reader)

            for row in itercsv_reader:
                swiperConfig = SwiperConfig()
                regex = re.compile(r'(MPM\w{9})\s(\d+)\s(\w+)\s\"(.*?)\"\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s\"(.*?)\"\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)')
                f = regex.findall(row[0])

                if len(f) != 0:
                    swiperConfig.setSwiperConfigfromList(f[0])
                    if options.get("action") == "WriteCSV":
                        editedconfig.write("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n"
                                           .format(swiperConfig.SiteID,swiperConfig.Version,swiperConfig.Swiper_MAC_address,swiperConfig.SwiperProfile,swiperConfig.MinCh,swiperConfig.MaxCh,swiperConfig.CoinVal,swiperConfig.Rate,swiperConfig.BnsCoin,swiperConfig.SigType,swiperConfig.Cancel,swiperConfig.Amex,swiperConfig.MustApp,swiperConfig.Fleet,swiperConfig.Debug,swiperConfig.BtnAdd,swiperConfig.AppDly,swiperConfig.PrchTyp,swiperConfig.SwiperName,swiperConfig.CoinTim,swiperConfig.ICTime,swiperConfig.AppTime,swiperConfig.BnsMode,swiperConfig.BnsCnt,swiperConfig.BnsTime,swiperConfig.PrchLim))
                    elif options.get("action") == "WriteSQL":
                        session = Session()
                        session.add(swiperConfig)
                        session.commit()
                else:
                    incompleteRows.append(row)
                line = line + 1
                print(line)
            print("\n\n\n\n")
            for i in incompleteRows:
                print(i)
                #line = line + 1
                #print(line, "   " ,row)
                #print(row)
                #print(f)


        print("Number of rows in Total: ", line)
        print("Number of rows Completed: ", line - len(incompleteRows))
    else:
        print("Error: This is not a CSV file")



if __name__ == '__main__':
    readCSV("configs.csv", action = "WriteSQL")
