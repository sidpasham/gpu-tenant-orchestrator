# Security Policy

## Supported Versions

This project is currently a prototype intended for local Rancher Desktop
development. No production support window is defined yet.

## Reporting A Vulnerability

Please do not open public issues for suspected vulnerabilities.

Before making the repository public, replace this section with the preferred
private reporting channel for the project owner or maintainer.

When reporting, include:

- Affected component or file path.
- Steps to reproduce.
- Expected and actual impact.
- Any relevant logs, requests, or configuration.

## Security Baseline

- Do not commit real credentials, kubeconfigs, cloud tokens, or private cluster
  endpoints.
- Run a secret scan before publishing the repository or rewriting history.
- Treat the local Kubernetes manifests as development manifests, not a
  production security model.
