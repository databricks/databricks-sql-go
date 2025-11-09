---
description: scrum sync up, make sure the JIRA board has the latest updata and sync up with code
---

### User Input
```text
$ARGUMENTS
```
 

You **MUST** consider the user input before proceeding. If empty, ask the user for a ticket number or task description to work on.

## Goal
Perform scrum actions, make sure the JIRA board reflect the latest project progress, and in sync with coding and design doc

## Required Information
- a JIRA EPIC or Store or Task or SubTask
- design docs
- source code

you must have a JIRA ticket number, if not provided ask for it. Design doc and source code location is an optoin, it maybe provided or you can find it out later


## Steps

## Gather Required Information
Please make sure you have all the information needed.
- make sure you have a JIRA ticket
- read the JIRA ticket, it may have child items
- try to locate the design doc and source code location


## Get a list of JIRA tickets
- if the JIRA ticket has sub items, such a EPIC ticket or a Story
- make sure you get all the ticket under it


## Check and update the JIRA ticket status
- for each candidate ticket, read the current status
- if the status if done, you can skip the checking
- check the code or design doc, see if the work is done
- if the work is done, add a comment and transit the ticket status to done
- if there are sill gaps, add a comment and log the findings
- make sure you do it for all leaf tickets

## Scrum summary
- after all of this, please provide a summary with current status and follow up that can be done.



