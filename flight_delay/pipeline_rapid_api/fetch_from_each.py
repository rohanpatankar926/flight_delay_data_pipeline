import csv
import os

class FetchData:
    def __init__(self):
        self.read_mode="r"
        self.write_mode="w"

    def fetch(self,data_src):
        if not os.path.exists(data_src):
            raise FileNotFoundError("Data source not found")
        
        for data in os.listdir(data_src):
            print(data)
            if data.endswith(".csv"):
                print(data)
                file_path=os.path.join(data_src,data)
                
                with open(file_path,self.read_mode) as read_data:
                    data_read=csv.reader(read_data)
                    header=next(data_read)
                    fetch_only_20=[rows for idx,rows in enumerate(data_read) if idx< 20000]
                if not os.path.exists(os.path.join(os.getcwd(),"data")):
                    os.makedirs(os.path.join(os.getcwd(),"data"),exist_ok=True)
                data_dest_name=os.path.join(os.getcwd(),"data",data)
                
                with open(data_dest_name,self.write_mode) as write_data:
                    data_write=csv.writer(write_data)
                    data_write.writerow(header)
                    data_write.writerows(fetch_only_20)
        return "Data fetched and saved successfully"

fetch_=FetchData()
fetch_.fetch(os.path.join(os.getcwd(),"src_data_dir"))
