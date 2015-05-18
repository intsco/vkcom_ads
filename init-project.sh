# Project creation script
# (for Tornado on Heroku)
#
# by Mike Dory | dory.me
# 11.12.11
# modified by Tedb0t | tedbot.com

# --------------------------

# git!
git init

# --------------------------

# set up the pip requirements
touch requirements.txt
echo "Tornado==4.1" >> requirements.txt
echo "pandas==0.15" >> requirements.txt

# set up the Procfile
touch Procfile
echo "web: python vkcom_ad_api.py" >> Procfile

# --------------------------

echo "Committing to Git"

git add .
git commit -m "Initial Commit"

echo "Creating Heroku app & pushing"

heroku create --stack cedar
git push heroku master

echo "All Done!"