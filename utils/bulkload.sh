# Check if the correct number of arguments are provided
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <table_name> <column_family>"
  exit 1
fi


TABLE_NAME=$1
COLUMN_FAMILY=$2

# Clean up any existing bulkload directories
rm -rf /tmp/bulkload

# Re-create the necessary directory structure
mkdir -p /tmp/bulkload/tsvdata

# Generate TSV data and save to the specified directory
python3 tsv_generator.py /tmp/bulkload/tsvdata

# Import TSV data to create HFiles for bulk loading
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -Dimporttsv.columns=HBASE_ROW_KEY,$COLUMN_FAMILY:col0,$COLUMN_FAMILY:col1,$COLUMN_FAMILY:col2,$COLUMN_FAMILY:col3,$COLUMN_FAMILY:col4,$COLUMN_FAMILY:col5,$COLUMN_FAMILY:col6,$COLUMN_FAMILY:col7,$COLUMN_FAMILY:col8,$COLUMN_FAMILY:col9 \
  -Dimporttsv.bulk.output=/tmp/bulkload/HFiles \
  $TABLE_NAME /tmp/bulkload/tsvdata/output.tsv

# Bulk load the generated HFiles into the HBase table
hbase completebulkload /tmp/bulkload/HFiles $TABLE_NAME