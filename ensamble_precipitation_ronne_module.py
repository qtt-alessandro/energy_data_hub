import openmeteo_requests
import polars as pl
from datetime import datetime, timedelta
import logging
import os
from openmeteo_sdk.Variable import Variable
from openmeteo_sdk.Aggregation import Aggregation
import requests_cache
from retry_requests import retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_fetcher.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('WeatherDataFetcher')

class WeatherDataFetcher:
    def __init__(self, latitude, longitude, cache_path='.cache', output_dir='data'):
        """
        Initialize the WeatherDataFetcher with coordinates and optional paths.
        
        Args:
            latitude (float): Latitude of the location
            longitude (float): Longitude of the location
            cache_path (str): Path for the cache file
            output_dir (str): Directory to save output files
        """
        self.latitude = latitude
        self.longitude = longitude
        self.url = "https://ensemble-api.open-meteo.com/v1/ensemble"
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Initializing WeatherDataFetcher for coordinates: {latitude}°N, {longitude}°E")
        
        # Setup cache and client
        try:
            cache_session = requests_cache.CachedSession(cache_path, expire_after=3600)
            retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
            self.client = openmeteo_requests.Client(session=retry_session)
            logger.debug(f"Cache initialized at {cache_path}")
        except Exception as e:
            logger.error(f"Failed to initialize cache: {str(e)}")
            raise
    
    def save_dataframe(self, df, start_date, end_date):
        """
        Save DataFrame to CSV with timestamp in filename.
        
        Args:
            df (polars.DataFrame): DataFrame to save
            start_date (str): Start date of the data
            end_date (str): End date of the data
        """
        timestamp =  datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"weather_data_{start_date}_to_{end_date}_{timestamp}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            df.write_csv(filepath)
            logger.info(f"DataFrame saved successfully to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save DataFrame: {str(e)}")
            raise
        
    def fetch_weather_data(self, start_date, end_date, models="icon_seamless"):
        """
        Fetch weather data for the specified date range.
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            models (str): Weather model to use
            
        Returns:
            tuple: (metadata_dict, polars.DataFrame)
        """
        logger.info(f"Fetching weather data from {start_date} to {end_date} using model: {models}")
        
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "hourly": ["precipitation", "rain"],
            "start_date": start_date,
            "end_date": end_date,
            "models": models
        }
        
        try:
            responses = self.client.weather_api(self.url, params=params)
            response = responses[0]
            logger.debug("Successfully received API response")
        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            raise
        
        # Extract metadata
        try:
            metadata = {
                "coordinates": f"{response.Latitude()}°N {response.Longitude()}°E",
                "elevation": f"{response.Elevation()} m asl",
                "timezone": f"{response.Timezone()} {response.TimezoneAbbreviation()}",
                "utc_offset": f"{response.UtcOffsetSeconds()} s"
            }
            logger.debug(f"Extracted metadata: {metadata}")
        except Exception as e:
            logger.error(f"Failed to extract metadata: {str(e)}")
            raise
        
        # Process hourly data
        try:
            hourly = response.Hourly()
            hourly_variables = [hourly.Variables(i) for i in range(hourly.VariablesLength())]
            
            hourly_precipitation = filter(lambda x: x.Variable() == Variable.precipitation, hourly_variables)
            hourly_rain = filter(lambda x: x.Variable() == Variable.rain, hourly_variables)
            
            # Create base DataFrame with time
            df = pl.DataFrame({
                'time': pl.datetime_range(
                    start= datetime.fromtimestamp(hourly.Time()),
                    end= datetime.fromtimestamp(hourly.TimeEnd()),
                    time_unit='ms',
                    interval='1h',
                    closed='left',
                    eager=True
                )
            })
            
            # Add precipitation data
            for variable in hourly_precipitation:
                member = variable.EnsembleMember()
                df = df.with_columns(
                    pl.Series(f"precipitation_member{member}", variable.ValuesAsNumpy())
                )
            
            # Add rain data
            for variable in hourly_rain:
                member = variable.EnsembleMember()
                df = df.with_columns(
                    pl.Series(f"rain_member{member}", variable.ValuesAsNumpy())
                )
            
            logger.info(f"Successfully processed data. DataFrame shape: {df.shape}")
            
            # Save the DataFrame
            self.save_dataframe(df, start_date, end_date)
            
            return metadata, df
            
        except Exception as e:
            logger.error(f"Failed to process hourly data: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        fetcher = WeatherDataFetcher(
            latitude=55.10091, 
            longitude=14.70664,
            output_dir='weather_data'  # Specify output directory
        )
        
        current_date = datetime.now()
        start_date = current_date.strftime("%Y-%m-%d")
        end_date = (current_date + timedelta(days=1)).strftime("%Y-%m-%d")

        metadata, df = fetcher.fetch_weather_data(
            start_date="2025-02-12",
            end_date="2025-02-13"
        )
        
        logger.info("Weather data fetching completed successfully")
        
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        raise