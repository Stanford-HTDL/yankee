printf "Dataset Name: "; read DATASETNAME
printf "Planet API Key: "; read PLANETAPIKEY

DATASETDIR=./data/$DATASETNAME/
mkdir -p $DATASETDIR
printf "Dataset directory: $DATASETDIR\n"

### Download vector files to make training data ###
GEOJSONBASEDIR=vector-files/geojson/verified-kilns/
GEOJSONDIR=$DATASETDIR$GEOJSONBASEDIR
mkdir -p $GEOJSONDIR
printf "Geojson directory: $GEOJSONDIR\n"

REMOTEDIR=vector-files/geojson/verified-kilns/

GCSPROJECT=som-htdatalab
printf "GCS Project: $GCSPROJECT\n"
GCSBUCKET=vector-training-data
printf "GCS Bucket: $GCSBUCKET\n"
GCSCREDENTIALS=_google_creds.json
CONDAENV=yankee
printf "Conda environment: $CONDAENV\n"

printf "Download geojson files from GCS? [y/n]"; read DLGEOJSON
if [ "$DLGEOJSON" != "${DLGEOJSON#[Yy]}" ] ;then
    conda run -n $CONDAENV python -m dl_dir_from_gcs \
    --bucket $GCSBUCKET --remote-dir $REMOTEDIR --local-dir $GEOJSONDIR \
    --gcs-credentials $GCSCREDENTIALS 
else
    if [ -z "$(ls -A $GEOJSONDIR)" ]; then
        echo "$GEOJSONDIR is empty."
    fi
fi

# if [ -z "$(ls -A $GEOJSONDIR)" ]; then
#     conda run -n $CONDAENV python -m dl_dir_from_gcs \
#     --bucket $GCSBUCKET --remote-dir $REMOTEDIR --local-dir $GEOJSONDIR \
#     --gcs-credentials $GCSCREDENTIALS 
# else
#    echo "Directory $GEOJSONDIR is not empty. Skipping..."
# fi

# ### Get PlanetScope imagery asset ids ###
# printf "Get PlanetScope Asset IDs? "; read GETIDS
# if [ "$GETIDS" != "${GETIDS#[Yy]}" ] ;then
#     echo Yes
# else
#     echo No
# fi
# MAXCLOUDCOVER=1.0
# ASSETNAMES="analytic_sr udm"
# DIFFDAYS=60
# ITEMTYPES="PSScene4Band"

# ASSETIDSMANIFESTPATH=$DATASETDIR/asset_ids.json

# ### Order assets ###
# REMOTEDIR=vector-files/geojson/verified-kilns/
# GCSPROJECT=som-htdatalab
# GCSBUCKET=raster-training-data
# GCSCREDENTIALSSTRING=_google_creds_str.txt

# ### Download assets from GCS ###
# IMAGERYDIR=$DATASETDIR/imagery/
# if [ -z "$(ls -A $IMAGERYPATH)" ]; then
#     conda run -n $CONDAENV python -m dl_ps_imagery \
#     --bucket $GCSBUCKET --remote-dir $REMOTEDIR --local-dir $IMAGERYPATH \
#     --gcs-credentials $GCSCREDENTIALS 
# else
#    echo "Directory $GEOJSONDIR is not empty. Skipping..."
# fi

### Make training-ready data ###
UIDLENGTH=12
EXPERIMENTUID=$(python -m get_uid --length $UIDLENGTH $*)
printf "Experiment UID: $EXPERIMENTUID\n"
TILEDIR=tiles/
TRAININGDIR=$DATASETDIR$TILEDIR
mkdir -p TRAININGDIR
printf "Training data directory: $TRAININGDIR\n"
ZOOMS="12 13 14 15 16 17"
printf "Quad Key Zoom levels used: $ZOOMS\n"

printf "Make training data? [y/n]"; read MAKEDATA
if [ "$MAKEDATA" != "${MAKEDATA#[Yy]}" ] ;then
    conda run -n $CONDAENV python -m make_training_data \
    --imagery-dir $IMAGERYDIR --zooms $ZOOMS \
    --output-dir TRAININGDIR
# else
    # if [ -z "$(ls -A $GEOJSONDIR)" ]; then
    #     echo "$GEOJSONDIR is empty."
    # fi
fi