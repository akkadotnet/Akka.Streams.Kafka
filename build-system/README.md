# Azure Pipelines Build Files
These `.yaml` files are used by Windows Azure DevOps Pipelines to help execute the following types of builds:

- Pull request validation on Linux (Mono / .NET Core)
- Pull request validation on Windows (.NET Framework / .NET Core)
NuGet releases are published by the GitHub Actions workflow in `.github/workflows/publish_nuget.yml`, which runs on every tag push and uses nuget.org Trusted Publishing.