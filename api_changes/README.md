# API changes

This folder contains a record of API changes in main.  It's indended for AI agents
to read in order to correctly apply merges or update dependencies and PRs. 

The updates are listed by date in the form: `update_<yymmdd>_<description>.md`

When applying a merge, rebase, or downstream update, all AI agents should first 
scan this folder to understand what relevant information may need to be applied.

When creating a PR that involves an API change potentially requiring downstream 
updates, an AI agent should create such a file.  This file should be humanly 
readable but contain enough information to correctly apply the needed changes 
without scanning the code.  


