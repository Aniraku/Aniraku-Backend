# Backend Technology Stack

Aniraku-Backend is a Go 1.24 service using Chi for HTTP routing and Zerolog for structured logging. Authentication is implemented through Supabase JWT/JWKS verification. The internal modules cover API routing, configuration, core models, embedding, network safety, and streaming-provider coordination. A Python auxiliary proxy is declared separately through `requirements.txt`, while Docker and Render files describe deployment paths.
