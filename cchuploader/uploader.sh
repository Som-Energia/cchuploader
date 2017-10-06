TODAY=`date +"%Y-%m-%d"`
echo 'Create directory at /tmp/cch/'$TODAY
mkdir /tmp/cch/$TODAY
. ~/conf/empowering_vars.sh 
PYTHONPATH=~/src/erp/server/sitecustomize/  ~/bin/python ~/src/cchuploader/cchuploader/uploader.py post /tmp/cch/$TODAY
rsync -az -e ssh /tmp/cch/$TODAY som@37.187.175.123:/home/som/$TODAY
echo 'Delete directory at /tmp/cch/'$TODAY
rm -r /tmp/cch/$TODAY
echo 'Done'
