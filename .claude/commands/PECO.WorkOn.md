---
description: Work on a JIRA ticket by understanding the ticket description, overall feature design, and scope of work, then implementing the solution.
---

### User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding. If empty, ask the user for a ticket number to work on.

## Goal
Implement a JIRA ticket based on the overall design documentation and the specific scope defined in the ticket.

## Steps

### Step 1: Understand the Overall Design
Locate and review the relevant design documentation:
- Use search tools to find the corresponding design doc based on the JIRA ticket content
- Read through the design doc thoroughly to understand the feature architecture
- Describe your findings and understanding of the problem
- Ask for confirmation before proceeding

### Step 2: Create a New Branch
Create a new stacked branch using `git stack create <branch-name>` for this work.
- Make sure you add the JIRA ticket into the branch name

### Step 3: Discuss Implementation Details
Plan the implementation approach:

**Important**: Focus on and limit the scope of work according to the JIRA ticket only.

**Important**: Don't start from scratch - there should already be a design doc related to this ticket. Make sure you understand it first, then add implementation details if needed.

Present your implementation plan and ask for confirmation. You may receive feedback on what to change - make sure you incorporate this feedback into your approach.

### Step 4: Implement the Solution
Write the implementation code:
- Keep code clean and simple
- Don't over-engineer or write unnecessary code
- Follow existing code patterns and conventions in the codebase

### Step 5: Write Tests
Ensure adequate test coverage:
- Write comprehensive tests for your implementation
- Run build and tests to ensure they pass
- Follow the testing guidelines in the CLAUDE.md file

### Step 6: Update the Design Documentation
After completing the code changes:
- Review the related design doc and update it to reflect any discrepancies with the actual implementation
- Document any important discussions or Q&As that occurred during implementation
- Ensure documentation remains accurate and up-to-date

### Step 7: Commit and Prepare PR
Finalize your changes:
- Commit the changes with a clear commit message
- Prepare a comprehensive PR title and description following the PR template
- Use `git stack push` to push changes and create the PR automatically
- also please update the  pr desc by following the pr desc guidline of the repo
