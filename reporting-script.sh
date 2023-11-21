s3_dir="s3://mot-non-production-simulations/applications/network_calibration"
output_dr="/Users/tri/Desktop/Work/monty-data-visualization"
scenarios=(
"outputs_network_v6_parking_cost" "outputs_network_v6_30min_timemutation"
)

for scenario in "${scenarios[@]}" 
do
   if [ ! -d "$output_dr/$scenario" ];then
        # echo "Create folder"
        mkdir "$output_dr/$scenario/"
   fi
   echo "Downloading for scenario $scenario..."
   aws s3 cp --recursive "$s3_dir/$scenario/reporting" "$output_dr/$scenario/"
done
