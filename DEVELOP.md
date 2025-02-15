# Publishing a New Version

Follow these steps to publish a new version of `@proddata/node-cratedb`:

## 1. Bump the Version

Update the version in your `package.json`. You can use the npm version command:

```bash
npm version patch  # or "minor" / "major" as appropriate
```

This command updates the version number, commits the change, and creates a new Git tag.

## 2. Run Quality Checks and Build

Ensure your code passes all quality checks and compiles correctly by running the CI script:

```bash
npm run ci
```

The ci script should run:
• Prettier (in check mode)
• ESLint
• Vitest tests
• The build (using tsc)

## 3. Commit and Push Changes

If not already committed, add your changes:

```bash
git add .
git commit -m "Prepare release vX.Y.Z"
```

Then push your changes and tags to GitHub:

```bash
git push && git push --tags
```

4. Publish to npm

Publish the new version to npm:

```bash
npm publish --access public
```

Make sure you are logged into your npm account (npm login) and that your package is configured to be public (if using a scoped package).

5. Verify the Release

Confirm that the new version is available on npm:

```bash
npm info @proddata/node-cratedb
```

Your package is now published and available for installation!

Happy coding, and thanks for using @proddata/node-cratedb!
