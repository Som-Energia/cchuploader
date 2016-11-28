TODAY=`date +"%Y-%m-%d"`
python cchuploader/uploader.py --path cch
rsync -az -e ssh cch $1:/home/som/${TODAY}
rm cch/*
