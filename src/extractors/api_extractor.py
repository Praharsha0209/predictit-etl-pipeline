"""
API Data Extractor
Handles API calls and data extraction
"""

import requests
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class APIExtractor:
    """Extract data from various APIs"""
    
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        """
        Initialize API extractor
        
        Args:
            base_url: Base URL for the API
            api_key: Optional API key for authentication
        """
        self.base_url = base_url
        self.api_key = api_key
        self.headers = self._build_headers()
    
    def _build_headers(self) -> Dict[str, str]:
        """Build request headers"""
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Airflow-ETL-Pipeline/1.0'
        }
        
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
        
        return headers
    
    def fetch_data(self, endpoint: str = '', params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Fetch data from API endpoint
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response as dictionary
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            logger.info(f"Fetching data from {url}")
            response = requests.get(
                url, 
                headers=self.headers, 
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched data: {len(str(data))} bytes")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from API: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON response: {str(e)}")
            raise
    
    def save_to_json(self, data: Dict[str, Any], filepath: str) -> str:
        """
        Save data to JSON file
        
        Args:
            data: Data to save
            filepath: Output file path
            
        Returns:
            File path where data was saved
        """
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            logger.info(f"Data saved to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving data to JSON: {str(e)}")
            raise
    
    def save_to_csv(self, data: Dict[str, Any], filepath: str) -> str:
        """
        Save data to CSV file
        
        Args:
            data: Data to save
            filepath: Output file path
            
        Returns:
            File path where data was saved
        """
        try:
            # Convert to DataFrame
            if isinstance(data, dict):
                # If data has a 'markets' or similar key with list of records
                if 'markets' in data:
                    df = pd.json_normalize(data['markets'])
                else:
                    df = pd.DataFrame([data])
            elif isinstance(data, list):
                df = pd.json_normalize(data)
            else:
                raise ValueError("Data must be dict or list")
            
            df.to_csv(filepath, index=False)
            logger.info(f"Data saved to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving data to CSV: {str(e)}")
            raise
    
    def extract_and_save(
        self, 
        output_path: str, 
        file_format: str = 'json',
        endpoint: str = '',
        params: Optional[Dict] = None
    ) -> str:
        """
        Extract data from API and save to file
        
        Args:
            output_path: Output file path
            file_format: Output format ('json' or 'csv')
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            Path to saved file
        """
        # Add timestamp to data
        data = self.fetch_data(endpoint, params)
        
        # Add metadata
        data_with_metadata = {
            'extracted_at': datetime.utcnow().isoformat(),
            'source': self.base_url,
            'data': data
        }
        
        # Save based on format
        if file_format.lower() == 'json':
            return self.save_to_json(data_with_metadata, output_path)
        elif file_format.lower() == 'csv':
            # For CSV, we save just the data part
            return self.save_to_csv(data, output_path)
        else:
            raise ValueError(f"Unsupported format: {file_format}")


# Example usage for PredictIt API
class PredictItExtractor(APIExtractor):
    """Specialized extractor for PredictIt API"""
    
    def __init__(self, api_url: str, output_dir: str):
        super().__init__(base_url=api_url)
        self.output_dir = output_dir

    def extract_and_save(self, output_format='json', filename='predictit_markets'):
        # Fetch data
        data = self.fetch_data()

        # Extract timestamped output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_filename = f"{filename}_{timestamp}.{output_format}"

        file_path = f"{self.output_dir}/{full_filename}"

        # Add metadata wrapper (Airflow-safe)
        final_payload = {
            "extracted_at": datetime.utcnow().isoformat(),
            "source": self.base_url,
            "data": data
        }

        # Save JSON always
        with open(file_path, "w") as f:
            json.dump(final_payload, f, indent=2)

        return final_payload


# Example usage for other popular APIs
class CryptoExtractor(APIExtractor):
    """Extract cryptocurrency data from CoinGecko"""
    
    def __init__(self):
        super().__init__(base_url='https://api.coingecko.com/api/v3/')
    
    def extract_crypto_prices(self, output_path: str, ids: str = 'bitcoin,ethereum') -> str:
        """Extract cryptocurrency prices"""
        params = {
            'ids': ids,
            'vs_currencies': 'usd',
            'include_24hr_change': 'true'
        }
        return self.extract_and_save(
            output_path, 
            file_format='json',
            endpoint='simple/price',
            params=params
        )


class WeatherExtractor(APIExtractor):
    """Extract weather data from OpenWeather"""
    
    def __init__(self, api_key: str):
        super().__init__(
            base_url='https://api.openweathermap.org/data/2.5/',
            api_key=api_key
        )
    
    def extract_weather(self, city: str, output_path: str) -> str:
        """Extract current weather for a city"""
        params = {
            'q': city,
            'appid': self.api_key,
            'units': 'metric'
        }
        return self.extract_and_save(
            output_path,
            file_format='json',
            endpoint='weather',
            params=params
        )