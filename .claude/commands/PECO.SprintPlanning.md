---
description: Sprint planning assistant that creates a story and sub-tasks for a 2-week sprint based on high-level and detailed design documents.
---

### User Input
```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding. If empty, ask the user for a ticket number or task description to work on.

## Goal
Create a comprehensive sprint plan including a JIRA story and sub-tasks for a 2-week sprint cycle.

## Required Information
- High-level design document
- Detailed design document(s)
- Current project status and past tickets

You can ask for the exact path to design documents, or search the current folder based on a task description provided by the user.

## Steps

### Step 1: Gather Required Information
Ensure you have all necessary documents and context. Ask for additional details if needed:
- Design document paths or descriptions
- Related EPIC or parent ticket information
- Any specific constraints or requirements

### Step 2: Understand the Problem
Analyze the current state of the project:
- Read through the design documents thoroughly
- Review past tickets and their status
- Examine the current codebase to understand implementation status
- Identify what has been completed and what remains to be done

### Step 3: Define the Sprint Goal
Based on your analysis, propose a realistic goal for the 2-week sprint. Discuss the proposed goal with the user to ensure alignment and feasibility before proceeding.

### Step 4: Break Down Work into Sub-Tasks
After goal confirmation, create a detailed breakdown of work items:
- Each task should ideally be scoped to ~2 days of work
- Focus strictly on items within the sprint goal scope
- Ensure tasks are concrete and actionable

### Step 5: Create JIRA Tickets
After user confirmation of the task breakdown, create:
- One parent story for the sprint goal
- Individual sub-tasks for each work item identified in Step 4


