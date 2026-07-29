# Spotify Insights CDK App

A Python CDK app for ingesting Spotify music data, generating email recaps and release-radar digests, and producing high-rotation playlists.

Overview
--------

- API ingestion
  - Periodically fetches and stores user top tracks/artists data.

- High rotation playlist
  - Generates and updates playlists of tracks with high play/rotation in a given timeframe.

- Email recap
  - Generates digest emails summarising recent top tracks.

- (Decommissioned) Release radar email
  - Curates new releases tailored to user(s) or segments and sends a weekly "release radar" email.
