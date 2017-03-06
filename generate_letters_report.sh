# export PGPASSWORD="shadman"
# schema_name=gt_demo_masterdemo11
psql -h 192.168.3.103 -U majordomo -d public_backups -tc "SELECT table_schema FROM information_schema.tables WHERE table_name like 'letter_letters';" -F"," -A > target_schema_list.csv
schema_name=gt_ujwal_netskope

# sed -i '1d;$d' target_schema_list.csv
while read line; do
  schema_name=$line
  psql -h 192.168.3.103 -U majordomo -d public_backups -v schema_name="$schema_name" -A -F"," -f letters.sql > "./letter_report/$schema_name.csv"
  sed -i '/row/d' ./letter_report/$schema_name.csv
done <target_schema_list.csv
  # sed '/row/{x;p;x;}'
# sed -n '/row/!p' ./letter_report/$schema_name.csv
# sed -i -e 's/^$/row/' ./letter_report/gt_bizproutcorporatesol_oanetworks.csv 
# python map-reduce.py

# rm target_schema_list.csv
# psql -d goal -U shadman -h 172.16.52.50 -v schema_name="$schema_name" -A -F"," -f letters.sql > letters_report.csv 
# message="Hello,\n\t please check the attachments for  Letters report.\t\n \t Thanks"
# echo -e "$message" | mail -t deepak.panda@greytip.com,  madhur@greytip.com, shadman@greytip.com -A letters_report.csv -s " Letters report"
