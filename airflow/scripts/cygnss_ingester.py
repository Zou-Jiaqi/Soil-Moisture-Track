from scripts import database_config, logger_config, property_manager # noqa: F401
import logging
from pathlib import Path
from datetime import datetime
import h5py
import re
import os
from scripts.entities.cygnss import CYGNSS
from geoalchemy2.shape import from_shape
from shapely.geometry import Point




class CYGNSSIngester:

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
                print('key: ', key, '---------------------------------------')


    def get_files(self, date=None):
        data_path = Path(self.path)
        pattern = 'cyg0[1-8].ddmi.s'
        if date is not None:
            pattern += date.strftime('%Y%m%d')

        files = [f.name for f in data_path.iterdir() if re.match(pattern, f.name) and f.is_file()]
        return files

    def ingest(self, filedate):
        files = self.get_files(filedate)

        incomplete_files = set(files)
        records = []
        for file in files:
            filepath = self.path + '/' + file
            # Read CYGNSS records
            try:
                with h5py.File(filepath, mode='r') as f:
                    lats = f['sp_lat'][()]
                    lons = f['sp_lon'][()]
                    delay = f['brcs_ddm_peak_bin_delay_row'][()]
                    incident_angle = f['sp_inc_angle'][()]
                    antenna_gain = f['gps_ant_gain_db_i'][()]
                    ref = f['reflectivity_peak'][()]

            except Exception as e:
                self.logger.exception(f"Failed to read {file}")
                self.logger.exception(e)
                continue

            rows, columns = ref.shape

            for i in range(rows):
                for j in range(columns):
                    reflectivity = ref[i][j]
                    gain = antenna_gain[i][j]
                    angle = incident_angle[i][j]
                    brcs_delay = delay[i][j]
                    lat = lats[i][j]
                    lon = lons[i][j]
                    if reflectivity == -9999 or gain == -9999 or angle == -9999 or brcs_delay == -99\
                            or lat == -9999 or lon == -9999:
                        continue

                    try:
                        record = CYGNSS(location=from_shape(Point(lon, lat), srid=4326),
                                    date=filedate, reflectivity=reflectivity.item(),
                                    incident_angle=angle.item(), antenna_gain=gain.item(),
                                    ddm_peak_delay=brcs_delay.item())
                    except Exception as e:
                        self.logger.exception(f"Failed to create record.")
                        self.logger.exception(e)
                        raise e

                    records.append(record)

            # save records into database
            with database_config.get_db() as session:
                try:
                    session.bulk_save_objects(records)
                    session.commit()  # commit makes it atomic
                    self.logger.info(f"{file} ingested")
                except:
                    session.rollback()  # rollback ensures atomicity
                    self.logger.exception(f"Failed to ingest {file}")
                    continue

            # delete ingested files
            file_to_be_delete = Path(filepath)
            if Path(file_to_be_delete).exists():
                file_to_be_delete.unlink()
                self.logger.info(f"{file} deleted.")

        if len(incomplete_files) != 0:
            msg = f"Fail to ingest: {incomplete_files}"
            self.logger.warning(msg)
            raise Exception(msg)

if __name__ == "__main__":
    ingester = CYGNSSIngester()
    # ingester.list_all_groups()
    ingester.ingest(datetime(2025, 6, 3))