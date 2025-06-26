import smap_ingest
import cygnss_ingest

datestr = "2025-06-01"

smap_ingest.ingest(datestr)
cygnss_ingest.ingest(datestr)