# sh run_init.sh 201601 201606
START=$1
END=$2
mkdir $END;python uploader_init.py post data/$END --start_month $START --end_month $END
