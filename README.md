# scrape_ao3
Scraping the Archive Of Our Own website
Note that this is not enough to fully replicate it on your own, but if you're really wanting to walk in my footsteps, this gives you a place to start. Or I may put something comprehensive up eventually;

To run on a device:
git clone https://github.com/whyisjohngalt/scrape_ao3.git
cd scrape_ao3/
chmod +x *
./setup_script.sh

or run as one line:
git clone https://github.com/whyisjohngalt/scrape_ao3.git; cd scrape_ao3/; chmod +x * ; ./setup_script.sh

Then, to scrape the meta_info, run:
python3 scrape_ao3_meta.py -i ID --client_id CLIENT_ID --client_secret CLIENT_SECRET --quota_project_id QUOTA_PROJECT_ID --refresh_token REFRESH_TOKEN --type TYPE

