# Switchbot Outdoor Meter Data Importer

The script allows you to add exported data from the Switchbot Outdoor Meter directly into an MySQL Database. This includes all data like the Temperature, Relative Humidity, Absolute Humidity as well as Dewpoint and VPD.

## Why not using the Switchbot API?
Unforuntately the Switchbot API only provides the following data fields:
```
humidity
temperature
```

However the Outdoor Meter gives more information which is also visible via the App: 
```
dewpoint
vpd
absoluteHumidity
```

When you export the data from the Switchbot App you will get a CSV File containing all relevant data entries: 
```
Timestamp
Temperature_Celsius(°C)
Relative_Humidity(%)
Absolute_Humidity(g/m³)
DPT_Celsius(°C)
VPD(kPa)
```

Since  those information are anyhow available with the data export this script allows you to historize the data into an MySQL Database. 

## Installation

Make sure to have Python installed. This script has been tested with Python 3.12.3.

Install the required packages with the provided requirements.txt file via the command line:

```bash
  pip install -r requirements.txt
```

### Setup of the script
After cloning the repository rename ```config_sample.py``` to ```config.py``` and open the file.

Make sure to fill in your database credentials in the ```dbConfig``` dictionary. Define the name of your Table in the ```config_params``` dictionary (```table_name```). 
_Hint:_ If the tables does not already exist the script will automatically create it. 


If you want to disable the statics output from the script set ```show_stats``` to ```False```.

## Data Export, preparation & execution
Please visit the documentation on the Binary Alps: Knowledge platform [via this link](https://kb.binary-alps.at/en/github/switchbot_data_importer).
    
## License

[GNU GPLv3](https://choosealicense.com/licenses/gpl-3.0/)

