import logging
import h5py
import os
from pathlib import Path
from datetime import datetime
from scripts.entities.smap import SMAP
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from scripts import database_config, logger_config, property_manager # noqa: F401

class SmapIngester:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.path = os.getenv("DATA_PATH").rstrip("/")
        self.db = database_config.get_db()

    def list_all_groups(self):
        files = self.get_files()
        if len(files) == 0:
            print("No files found")
            return
        file = files[0]
        filepath = self.path + "/" + file
        with h5py.File(filepath, mode='r') as f:
            for key in f.keys():
                dataset = f[key]
                print('key: ', key, '---------------------------------------')
                for item in dataset:
                    print('data:', item)

    def get_files(self, date=None):
        data_path = Path(self.path)
        pattern = 'SMAP_L3_SM_P_E_'
        if date is not None:
            pattern +=  date.strftime('%Y%m%d')

        files = [f.name for f in data_path.iterdir() if f.name.startswith(pattern) and f.is_file()]
        return files


    def init_latlon(self):
        files = self.get_files()

        if len(files) == 0:
            msg = "No SMAP files found. Unable to initialize latitude and longitude."
            self.logger.error(msg)
            raise Exception(msg)

        for file in files:
            filepath = self.path + '/' + file
            try:
                lat = []
                lon = []
                with h5py.File(filepath, mode='r') as f:
                    lat = self._init_latitude(f)
                    lon = self._init_longitude(f)
                with h5py.File(f'{self.path}/latlon.h5', mode='w') as latlon:
                    latlon.create_dataset('latitude', data=lat)
                    latlon.create_dataset('longitude', data=lon)
                    return
            except Exception as e:
                self.logger.warning(f"Failed to extract from {file}: {e}")
        else:
            self.logger.exception("All files failed for lat/lon extraction")
            raise RuntimeError("No valid file found for lat/lon extraction")


    def _init_latitude(self, h5):

        lat_info = h5['Soil_Moisture_Retrieval_Data_PM/latitude_centroid_pm']
        rows, columns = lat_info.shape
        lat = [-9999] * rows
        for i, l in enumerate(lat_info):
            for x in l:
                if x != -9999:
                    lat[i] = x
                    break
            if lat[i] == -9999:
                raise Exception("No valid latitude found")
        return lat

    def _init_longitude(self, h5):

        lon_info = h5['Soil_Moisture_Retrieval_Data_PM/longitude_centroid_pm']
        rows, columns = lon_info.shape
        lon = [-9999] * columns
        for l in lon_info:
            for i in range(columns):
                lon[i] = max(lon[i], l[i])
        if -9999 in lon:
            raise Exception("No valid longitude found")
        return lon


    def ingest(self, filedate):
        latlon_path = Path(self.path + '/latlon.h5')
        if not latlon_path.exists():
            self.init_latlon()

        files = self.get_files(filedate)
        lat = []
        lon = []
        with h5py.File(latlon_path, mode='r') as latlon:
            lat = latlon['latitude'][()]
            lon = latlon['longitude'][()]
            # lat = [x.item() for x in latlon['latitude'][()]]
            # lon = [x.item() for x in latlon['longitude'][()]]


        rows, columns = len(lat), len(lon)
        result = [[0] * columns for _ in range(rows)]
        counts = [[0] * columns for _ in range(rows)]

        for file in files:
            filepath = self.path + '/' + file
            with h5py.File(filepath, mode='r') as f:
                sm_am = f['Soil_Moisture_Retrieval_Data_AM/soil_moisture']
                sm_pm = f['Soil_Moisture_Retrieval_Data_PM/soil_moisture_pm']
                quality_am = f['Soil_Moisture_Retrieval_Data_AM/retrieval_qual_flag']
                quality_pm = f['Soil_Moisture_Retrieval_Data_PM/retrieval_qual_flag_pm']
                for i in range(rows):
                    sm_am_r = sm_am[i]
                    sm_pm_r = sm_pm[i]
                    quality_am_r = quality_am[i]
                    quality_pm_r = quality_pm[i]
                    for j in range(columns):
                        if quality_am_r[j] == 0 or quality_am_r[j] == 8:
                            result[i][j] += sm_am_r[j]
                            counts[i][j] += 1
                        if quality_pm_r[j] == 0 or quality_pm_r[j] == 8:
                            result[i][j] += sm_pm_r[j]
                            counts[i][j] += 1

        records = []
        for i in range(rows):
            for j in range(columns):
                if counts[i][j] > 0:
                    soil_moisture = result[i][j] / counts[i][j]
                    record = SMAP(location=from_shape(Point(lon[j], lat[i]), srid=4326), date=filedate, soil_moisture=soil_moisture.item())
                    records.append(record)

        with database_config.get_db() as session:
            try:
                session.bulk_save_objects(records)
                session.commit()  # commit makes it atomic
                self.logger.info(f"SMAP data ingested: {files}")
            except:
                session.rollback()  # rollback ensures atomicity
                self.logger.exception("Failed to ingest SMAP data")
                raise Exception("Failed to ingest SMAP data")

        for file in files:
            filepath = Path(self.path + '/' + file)
            if filepath.exists():
                filepath.unlink()
                logging.info(f"{file} deleted.")


if __name__ == "__main__":
    smap = SmapIngester()
    smap.ingest(datetime(2025, 6, 1))