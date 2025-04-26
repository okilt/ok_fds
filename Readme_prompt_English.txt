# README Generation Request

## Role
You are an experienced technical writer and developer. Your task is to generate a high-quality README.md file for a software project based on the provided information.

## Goal
Generate an exceptionally clear and comprehensive README.md file in Markdown format based on the information below. The README should greatly facilitate understanding, usage, contribution, and awareness of potential issues for both **end-users** and **developers**.

## Provided Information

1.  **Project Name:**
    `[Enter project name here]`

2.  **Project Overview:** (What the project does, its main purpose, the problem it solves, described specifically)
    `[Enter a more detailed description of the project here]`

3.  **Target Audience:** (Optional: Specify the intended users and their prerequisite knowledge)
    `[Example: Data scientists experienced with Python data processing, Front-end developers familiar with React, etc.]`

4.  **Folder Structure:** (Output from `tree` command or a manually described tree structure)
    ```
    [Paste or describe the folder structure here]
    ```

5.  **Key Folder/File Descriptions:** (Explain the role of each element corresponding to the folder structure. Include design intent if applicable)
    *   `[Folder/File Name 1]`: `[Description of its role and content. Design rationale, etc.]`
    *   `[Folder/File Name 2]`: `[Description of its role and content]`
    *   ... (Items important for understanding the project)

6.  **Key Code Snippets (Optional):** (Provide relevant code excerpts for core logic, configuration, API endpoints, etc., to make the README more concrete. **Ensure all sensitive information is removed or masked.**)
    *   **File Path:** `[Example: src/core/processor.py]`
        ```python
        # [Paste key class/function code from processor.py here]
        # Example: Core data processing logic
        class DataProcessor:
            def __init__(self, config):
                # ...
            def run(self, data):
                # ...
        ```
    *   **File Path:** `[Example: routes/api.js]`
        ```javascript
        // [Paste key API endpoint definitions from api.js here]
        // Example: Get user info API
        router.get('/users/:id', async (req, res) => {
          // ...
        });
        ```
    *   **File Path:** `[Example: config/default.yaml]`
        ```yaml
        # [Paste example of key configuration items here]
        api_key: YOUR_API_KEY_HERE # MUST remove/mask
        database:
          host: localhost
          port: 5432
        ```
    *   ... (Add more as needed)

7.  **Key Features:** (What the software can do, listed as specific bullet points from a user's perspective)
    *   `[Feature 1: Specific action or result]`
    *   `[Feature 2: Specific action or result]`
    *   ...

8.  **Technology Stack:** (List main languages, frameworks, libraries, databases, infrastructure, including versions if known)
    *   `[Language: e.g., Python 3.10+, Node.js 18.x]`
    *   `[Framework: e.g., FastAPI 0.9x, Next.js 13]`
    *   `[Library: e.g., Pandas, SQLAlchemy, Zustand]`
    *   `[Database: e.g., PostgreSQL 15, Redis]`
    *   `[Infrastructure: e.g., Docker, AWS (S3, EC2)]`
    *   ...

9.  **Installation / Setup Instructions:** (Specific steps for users to set up the environment and get started. Include prerequisites.)
    *   `Prerequisites: [Example: Docker Desktop, Python 3.10+, Node.js 18+]`
    *   `Step 1: Clone the repository`
        ```bash
        git clone [Repository URL]
        cd [Project Name]
        ```
    *   `Step 2: Install dependencies`
        ```bash
        # Example: For Python
        python -m venv venv
        source venv/bin/activate  # Windows: venv\Scripts\activate
        pip install -r requirements.txt
        # Example: For Node.js
        npm install
        ```
    *   `Step 3: Set up environment variables`
        `[Example: Copy .env.example to .env and fill in the required values.]`
    *   `Step 4: Database Migrations (if applicable)`
        ```bash
        # Example: Using Alembic
        alembic upgrade head
        ```
    *   ...

10. **Basic Usage:** (Specific command examples to run the software, configuration methods, a simple tutorial, API endpoint examples, etc.)
    *   `How to Run:`
        ```bash
        # Example: Run a Python script
        python src/main.py --input data.csv --output results.json
        # Example: Start a web server
        uvicorn src.app:app --reload
        # Example: Run an npm script
        npm run dev
        ```
    *   `Configuration Example:`
        `[Example: Adjust the processing threshold by changing the `threshold` value in config.yaml.]`
    *   `API Endpoint Examples (if applicable):`
        `[Example: Get a list of items via `GET /api/items`.]`
        `[Example: Create an item via `POST /api/items` with the following JSON body: ...]`

11. **Deployment (Optional):** (How to deploy the application to a production environment, considerations. Docker image build steps, server config examples, etc.)
    *   `Deployment with Docker:`
        ```bash
        # Build the image
        docker build -t [image-name] .
        # Run the container
        docker run -d -p 8000:8000 --env-file .env [image-name]
        ```
    *   `Server Requirements:`
        `[Example: Requires Nginx, Gunicorn/PM2, Supervisor, etc.]`
    *   `Deployment to PaaS:`
        `[Example: Instructions or links for deploying to Heroku, Vercel, AWS Elastic Beanstalk]`
    *   `Important Notes:`
        `[Example: Ensure DEBUG=False is set in production environments.]`

12. **For Developers:** (Information for those looking to contribute to development)
    *   `Development Setup:`
        `[Example: Install development dependencies with `pip install -r requirements-dev.txt`.]`
        `[Example: Set up pre-commit hooks: `pre-commit install`]`
    *   `Running Tests:`
        ```bash
        # Example: pytest
        pytest tests/
        # Example: npm test
        npm test
        ```
    *   `Code Formatting / Linting:`
        ```bash
        # Example: Black, Flake8, Prettier
        black .
        flake8 .
        npm run lint
        ```
    *   `Branching Strategy:`
        `[Example: The main branch is protected. Develop features by branching off the develop branch.]`
    *   `Coding Standards:`
        `[Example: Follow PEP 8, refer to the xxx style guide.]`
    *   `Creating Database Migrations (if applicable):`
        ```bash
        # Example: Alembic
        alembic revision --autogenerate -m "Add new table"
        ```

13. **Contributing:** (Optional: Specific steps for reporting bugs, requesting features, submitting pull requests)
    *   `Issues:` `[Please use the GitHub Issues templates for bug reports and feature requests.]`
    *   `Pull Requests:` `[Ensure there is a corresponding Issue. Ensure tests pass. Follow the PR template.]`

14. **Known Issues / Limitations (Optional):** (Currently recognized problems, performance bottlenecks, unimplemented features, etc.)
    *   `[Example: Processing very large datasets (>1GB) may lead to high memory usage.]`
    *   `[Example: Functionality on Windows environments is partially untested.]`
    *   `[Example: Authentication features are currently under development.]`

15. **License:** (Optional: Project's license)
    `[Example: This project is licensed under the MIT License. See the LICENSE file for details.]`

16. **Other:** (Optional: Contact info, references, acknowledgments, links to screenshots or demos, etc.)
    *   `Contact:` `[Please use GitHub Issues for problems.]`
    *   `References:` `[Links to relevant documentation or papers.]`
    *   `Acknowledgments:` `[Thanks to contributors or libraries used.]`
    *   **(Recommended)** `Screenshots/Demo:` `[Include links to screenshots or a live demo site here if available.]`
    *   **(Recommended)** `Badges:` `[Recommend adding badges for CI/CD status, coverage, license, etc., here.]`

## Output Requirements

*   **Format:** Markdown (`README.md`)
*   **Structure:** Cover the items from "Provided Information" in a logical and clear structure. The order doesn't strictly need to follow the input; prioritize readability.
*   **Language:** English
*   **Tone:**
    *   User-facing sections: Clear, concise, welcoming, helpful. Explain jargon briefly if necessary.
    *   Developer-facing sections: Technically accurate, concise.
*   **Readability:** Use bullet points, code blocks (with language specification recommended), emphasis, section breaks, etc., appropriately to create a highly readable document.
*   **Comprehensiveness and Specificity:** Reflect the provided information as much as possible. Use the "Key Code Snippets" to enrich examples in "Basic Usage" and "For Developers." Be mindful of redundancy and exercise judgment in information selection.
*   **Output Format:** **Enclose the *entire* generated README content within a single Markdown code block like the example below. This ensures the Markdown source code can be copied directly from the chat interface without being rendered.**
    ```markdown
    (Place the entire generated README.md content here)
    ```

---

Now, please generate the best possible README.md based on the information provided above.
