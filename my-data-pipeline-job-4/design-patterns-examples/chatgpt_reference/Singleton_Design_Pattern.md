```mermaid
flowchart TD
    A[Client calls Singleton.getInstance] --> B{Is instance created?}
    B -- Yes --> C[Return existing instance]
    B -- No --> D[Create new instance]
    D --> E[Store instance in class variable]
    E --> F[Return new instance]
