import requests

url = "https://flight_delay.p.rapidapi.com/data"

headers = {
	"X-RapidAPI-Key": "c12a4fcf8emshf19712541926cdcp19ec27jsn683bd634421e",
	"X-RapidAPI-Host": "flight_delay.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers)

print(response.text)
import json
with open('flight_delay_data.json', 'w') as f:
	json.dump(json.loads(response.text), f)