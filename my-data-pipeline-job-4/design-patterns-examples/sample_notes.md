
111

Below is a table that lists the design patterns used in the software development context, along with a brief description, guidance for when to use each pattern, and situations in which they may not be appropriate.

| Serial Number | Name of Design Pattern    | One Line Description                                                                                              | When to Use This Design Pattern                                                                                                                                       | When Not to Use This Design Pattern                                                                                                                           |
|---------------|---------------------------|-------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1             | Pipeline Pattern          | Arranges processing into a series of sequential, independent stages where each stage passes its output forward.   | When processing data in steps where each stage transforms or acts upon the input and can be independently maintained or replaced.                                      | When the processing stages are tightly coupled or interdependent, making linear sequencing impractical.                                                         |
| 2             | Strategy Pattern          | Defines a family of algorithms, encapsulating each one so that they can be interchanged at runtime.                  | When multiple alternative algorithms or behaviors are needed within the same interface and selection needs to be done dynamically.                                     | When there is no need to vary behavior at runtime or when the overhead of extra classes outweighs the benefits of separation.                                    |
| 3             | Factory Pattern           | Provides an interface for creating objects, letting subclasses decide which class to instantiate.                  | When a system needs to create objects dynamically and decouples the client code from the concrete classes being instantiated.                                         | When object creation is simple and does not require future extension, adding unnecessary complexity to the codebase.                                           |
| 4             | Adapter Pattern           | Converts one interface into another that a client expects, enabling incompatible interfaces to work together.      | When integrating with legacy or third-party code where interfaces do not match, but a conversion or wrapper can reconcile the difference.                           | When both systems already have compatible interfaces or when added indirection complicates the design unnecessarily.                                             |
| 5             | Decorator Pattern         | Dynamically attaches additional responsibilities to an object without modifying its structure.                     | When additional features or behavior needs to be added to objects at runtime without altering their original code or inheritance hierarchy.                         | When the functionality can be achieved more simply by subclassing, or when the extra layers add too much complexity without sufficient benefit.                |
| 6             | Chain of Responsibility   | Passes a request along a chain of handlers until one of them handles the request.                                  | When multiple handlers may process a request and the exact handler is not known in advance, allowing dynamic assignment of responsibilities.                        | When it is clear that a single object should handle the request or when the order of processing is critical and must be fixed.                                    |
| 7             | Template Method Pattern   | Defines the skeleton of an algorithm in an operation, deferring some steps to subclasses.                           | When the overall algorithm remains constant but some steps can vary and should be defined by subclasses.                                                               | When all steps of an algorithm are expected to change, making the rigid structure of the template method pattern a hindrance.                                     |
| 8             | Observer Pattern          | Establishes a one-to-many dependency between objects so that when one object changes state, its dependents are notified. | When an object's state change needs to trigger updates in multiple other objects, ensuring a loosely coupled system where observers can be added or removed dynamically. | When real-time notifications are not required or when the overhead of managing many observers outweighs the benefits.                                           |
| 9             | Circuit Breaker Pattern   | Prevents a system from performing an operation that is likely to fail, cutting off requests to a failing service temporarily. | When calling external services that may fail unpredictably, to avoid cascading failures and allow time for recovery.                                                   | When the external dependency is stable and failures are rare, making extra protection unnecessary, or when failure handling can be managed through simpler logic. |
| 10            | Configuration Pattern     | Centralizes configuration settings in one place for easy management and modification without altering code.         | When managing numerous configuration parameters that might change between environments, and you want to avoid scattering config details throughout the codebase.   | When there are only a few configuration options that do not change frequently, in which case a simpler approach might suffice.                                   |

This table provides a concise overview of common design patterns along with guidelines for their usage and potential pitfalls.


222


Here's a comprehensive table of software design patterns with their key details:

| #  | Design Pattern         | Description                                                                 | When to Use                                                                 | When Not to Use                                                          |
|----|------------------------|-----------------------------------------------------------------------------|-----------------------------------------------------------------------------|---------------------------------------------------------------------------|
| 1  | Singleton              | Restricts class initialization to ensure only one instance exists          | When global access to a single instance is required                        | When multiple instances might be needed later                            |
| 2  | Factory                | Delegates object creation to a separate factory class                      | When object creation logic needs encapsulation                             | For simple object creation with no complex logic                         |
| 3  | Abstract Factory       | Creates families of related objects without specifying concrete classes     | When dealing with multiple product families                                 | For single product type scenarios                                        |
| 4  | Builder                | Constructs complex objects step-by-step                                    | When creating objects with many optional parameters                        | For simple objects with fixed structure                                  |
| 5  | Prototype              | Creates new objects by cloning existing instances                          | When object creation is costly compared to cloning                         | When deep copying isn't required                                         |
| 6  | Adapter                | Bridges incompatible interfaces between classes                            | When integrating legacy systems with new interfaces                        | When interfaces already match                                            |
| 7  | Composite              | Treats individual objects and compositions uniformly                       | For part-whole hierarchies (e.g., GUI components)                           | When dealing with non-hierarchical structures                            |
| 8  | Proxy                  | Controls access to another object                                          | For lazy initialization, access control, or logging                        | When direct access is sufficient                                         |
| 9  | Flyweight              | Shares common state between objects to reduce memory usage                 | When dealing with many similar objects (e.g., text formatting)              | For objects with unique states                                           |
| 10 | Facade                 | Provides simplified interface to complex subsystems                        | To reduce subsystem complexity for clients                                  | When direct subsystem access is necessary                                |
| 11 | Bridge                 | Decouples abstraction from implementation                                  | When platform-independent abstractions are needed                          | For tightly coupled systems                                               |
| 12 | Decorator              | Dynamically adds responsibilities to objects                               | For flexible extension of functionality                                    | When using static inheritance suffices                                   |
| 13 | Template Method        | Defines algorithm skeleton with steps implemented in subclasses            | When multiple algorithms share similar structures                           | For algorithms with no common structure                                  |
| 14 | Mediator               | Centralizes complex communication between objects                          | When object interactions become too complex                                 | For simple direct communication scenarios                                |
| 15 | Chain of Responsibility| Passes requests through a chain of handlers                                | When multiple objects might handle a request                                | When request has single handler                                          |
| 16 | Observer               | Notifies dependent objects about state changes                             | For event-driven systems or publish-subscribe scenarios                     | When tight coupling is acceptable                                        |
| 17 | Strategy               | Encapsulates interchangeable algorithms                                    | When multiple algorithm variants exist                                     | For single algorithm implementations                                     |
| 18 | Command                | Encapsulates requests as objects                                           | For queuing requests, undo/redo functionality                              | For simple direct execution                                              |
| 19 | State                  | Changes object behavior based on internal state                            | When object has multiple states with different behaviors                    | For stateless objects                                                    |
| 20 | Visitor                | Separates algorithms from object structures                                | When adding operations to existing classes without modifying them           | When object structure changes frequently                                 |
| 21 | Interpreter            | Implements language grammar using class hierarchy                          | For domain-specific language implementation                                 | For general-purpose language parsing                                     |
| 22 | Iterator               | Provides sequential access to collection elements                          | When abstracting collection traversal logic                                 | For simple collections with direct access                                |
| 23 | Memento                | Captures and restores object state                                          | For undo/redo functionality or snapshots                                   | When object state is simple or immutable                                 |
| 24 | Null Object            | Provides default behavior for absent objects                               | To avoid null checks in client code                                         | When null references are acceptable                                      |
| 25 | Private Class Data     | Restricts access to class mutators                                          | When enforcing immutability or access control                               | For mutable objects without access restrictions                          |

**Key Considerations:**
- Creational patterns (1-5) optimize object creation[1][5]
- Structural patterns (6-12) manage object composition[1][5]
- Behavioral patterns (13-25) handle object collaboration[1][5]
- Use patterns when facing recognized problems[3][4], but avoid premature implementation[4]
- Prefer simple solutions over patterns for straightforward problems[4]
- Patterns improve maintainability but add complexity[3][4]

Citations:
[1] https://www.digitalocean.com/community/tutorials/gangs-of-four-gof-design-patterns
[2] https://github.com/DovAmir/awesome-design-patterns
[3] https://refactoring.guru/design-patterns/what-is-pattern
[4] https://arjancodes.com/blog/effective-use-of-design-patterns-in-software-development/
[5] https://www.scaler.com/topics/design-patterns/types-of-design-pattern/
[6] https://refactoring.guru/design-patterns/catalog
[7] https://www.tutorialspoint.com/design_pattern/design_pattern_overview.htm
[8] https://dev.to/fpaghar/design-pattern-list-c70
[9] https://en.wikipedia.org/wiki/Software_design_pattern
[10] https://learningdaily.dev/the-7-most-important-software-design-patterns-d60e546afb0e
[11] https://catalogue.projectsbyif.com
[12] https://www.freecodecamp.org/news/the-basic-design-patterns-all-developers-need-to-know/
[13] https://java-design-patterns.com/patterns/
[14] https://hillside.net/patterns/patterns-catalog
[15] https://en.wikipedia.org/wiki/Software_design_pattern
[16] https://learn.microsoft.com/en-us/azure/architecture/patterns/
[17] https://refactoring.guru/design-patterns
[18] https://www.youtube.com/watch?v=BJatgOiiht4
[19] https://www.blazemeter.com/blog/software-design-patterns
[20] https://sourcemaking.com/design_patterns
[21] https://softwareengineering.stackexchange.com/questions/49379/when-should-i-use-and-not-use-design-patterns
[22] https://refactoring.guru/design-patterns/examples
[23] https://dev.to/leoat12/design-patterns-use-or-not-to-use-22h6
[24] https://ryax.tech/design-pattern-what-is-it-and-why-use-it/
[25] https://stackoverflow.com/questions/85272/how-do-you-know-when-to-use-design-patterns
[26] https://www.youtube.com/watch?v=tv-_1er1mWI
[27] https://www.scaler.com/topics/software-engineering/design-patterns-in-software-engineering/
[28] https://www.reddit.com/r/SoftwareEngineering/comments/l5ys8n/how_to_use_and_not_use_design_patterns/
[29] https://www.indeed.com/career-advice/career-development/types-of-design-patterns
[30] https://refactoring.guru/design-patterns/why-learn-patterns
[31] https://www.digitalocean.com/community/tutorials/java-design-patterns-example-tutorial
[32] https://swimm.io/learn/system-design/the-top-7-software-design-patterns-you-should-know-about
[33] https://lfcps.org/orbital/Orbital-doc/api/Patterns/Design/index.html
[34] https://alexkondov.com/why-do-we-use-design-patterns/

---
Answer from Perplexity: pplx.ai/share


333

# Software Design Patterns

| No. | Pattern Name | Description | When to Use | When Not to Use |
|-----|-------------|-------------|------------|-----------------|
| 1 | Singleton | Ensures a class has only one instance with global access point | When exactly one object is needed to coordinate actions across the system | When shared state is undesirable or testing isolation is important |
| 2 | Factory Method | Defines an interface for object creation, but lets subclasses decide which class to instantiate | When a class can't anticipate the type of objects it must create | When the hierarchy of products is unnecessary or overly complex |
| 3 | Abstract Factory | Provides an interface for creating families of related objects | When the system needs to be independent from how its products are created | When families of related products aren't clearly defined |
| 4 | Builder | Separates the construction of a complex object from its representation | When an object has numerous parameters and configurations | For simple objects with few variations |
| 5 | Prototype | Creates new objects by copying an existing object | When the cost of creating a new object is expensive | When objects contain resources that can't be copied |
| 6 | Adapter | Allows incompatible interfaces to work together | When existing classes can't be modified but must work with new interfaces | When adapting would add unnecessary complexity |
| 7 | Bridge | Separates an abstraction from its implementation so both can vary independently | When both abstraction and implementation should be extensible | For simple class hierarchies with minimal variation |
| 8 | Composite | Composes objects into tree structures to represent part-whole hierarchies | When clients should treat individual objects and compositions uniformly | When operations on the container aren't applicable to individual components |
| 9 | Decorator | Attaches additional responsibilities to objects dynamically | When extending functionality without modifying existing code | When numerous small decoration variants lead to complex hierarchies |
| 10 | Facade | Provides a simplified interface to a complex subsystem | When you need to provide a simple entry point to a complex system | When fine-grained control over subsystems is required |
| 11 | Flyweight | Uses sharing to support large numbers of fine-grained objects efficiently | When many similar objects cause memory issues | When each object's state must be unique |
| 12 | Proxy | Provides a surrogate or placeholder for another object | When access control, lazy initialization, or remote access is needed | When adding a layer of indirection creates unnecessary complexity |
| 13 | Chain of Responsibility | Passes a request along a chain of handlers | When multiple objects may handle a request and the handler isn't known beforehand | When requests must always be handled by a specific handler |
| 14 | Command | Encapsulates a request as an object | When parameterizing operations, queueing requests, or supporting undo | For simple operations with no need for history or queueing |
| 15 | Interpreter | Defines a grammar for a language and interprets sentences | When parsing and evaluating a domain-specific language | For complex grammars where parser generators would be more suitable |
| 16 | Iterator | Provides sequential access to elements without exposing underlying structure | When traversing different collection types with a uniform interface | When simple direct access is sufficient |
| 17 | Mediator | Defines an object that encapsulates how a set of objects interact | When communication between objects becomes complex | When components can communicate simply without coordination |
| 18 | Memento | Captures and externalizes an object's internal state | When implementing undo mechanisms or history features | When tracking state changes would be prohibitively expensive |
| 19 | Observer | Defines a one-to-many dependency between objects | When changes to one object require automatic updates to other objects | When notifications would create circular update cascades |
| 20 | State | Allows an object to alter its behavior when its internal state changes | When an object's behavior depends on its state | For simple state transitions not requiring full context switch |
| 21 | Strategy | Defines a family of algorithms, encapsulates each one, and makes them interchangeable | When different variants of an algorithm are needed | When algorithm variation is minimal or not required at runtime |
| 22 | Template Method | Defines the skeleton of an algorithm, deferring some steps to subclasses | When common algorithm structure exists with varying implementations | When variations in algorithm steps are minimal |
| 23 | Visitor | Separates an algorithm from an object structure on which it operates | When operations on a complex structure vary, but structure doesn't | When classes frequently change or visitors must access private members |
| 24 | Repository | Mediates between the domain and data mapping layers | When centralizing data access logic for domain objects | For simple applications with straightforward data access needs |
| 25 | Unit of Work | Maintains a list of objects affected by a business transaction | When tracking changes before persisting to a database | For simple applications with single-object transactions |
| 26 | Dependency Injection | Provides dependencies to objects instead of having them create dependencies | When decoupling components and enabling testability | For small applications where complexity outweighs benefits |
| 27 | Module | Encapsulates related components to provide better organization | When breaking large systems into manageable, loosely coupled pieces | For small applications where modularization adds overhead |
| 28 | MVC (Model-View-Controller) | Separates application into three interconnected components | When separating data, user interface, and control logic | For simple UIs or applications with minimal interaction |
| 29 | MVVM (Model-View-ViewModel) | Separates UI from business logic with an intermediary ViewModel | When binding UI elements to observable data from the model | For simple UIs that don't justify the additional abstraction |
| 30 | Specification | Encapsulates business rules that define an object filter | When complex filtering logic needs to be encapsulated and composed | For simple filtering that doesn't require composition or reuse |
| 31 | Data Access Object (DAO) | Abstracts and encapsulates data access mechanisms | When isolating database operations from business logic | For simple applications with minimal data access requirements |
| 32 | Service Locator | Centralizes service instance creation and access | When components need access to a common service registry | When simpler dependency injection would suffice |
| 33 | Lazy Loading | Defers initialization of an object until it's needed | When resource-intensive objects shouldn't be loaded until necessary | When objects are always needed immediately or performance impact is negligible |
| 34 | Event Aggregator | Acts as a central hub for events from multiple sources | When decoupling event producers from consumers | For simple applications with straightforward event flows |
| 35 | Circuit Breaker | Prevents system failure when a service is unresponsive | When handling potential failures in distributed systems | For non-distributed systems or where failures are rare and recoverable |
| 36 | Saga | Manages failures by coordinating multiple transactions | When implementing complex business processes across services | For simple, atomic transactions within a single service |
| 37 | Bulkhead | Isolates elements of an application to prevent failures from spreading | When critical system components need protection from cascading failures | In tightly coupled systems where isolation isn't feasible |
| 38 | Throttling | Controls the rate at which operations are performed | When protecting resources from excessive usage | When maximum throughput is always required |
| 39 | Retry | Attempts an operation multiple times in case of failure | When operations may occasionally fail but eventually succeed | For operations that will consistently fail or have side effects |
| 40 | Cache-Aside | Loads data into a cache on demand | When selectively caching frequently accessed items | When data changes frequently or when memory consumption is a concern |
| 41 | Anti-Corruption Layer | Translates between different models in separate subsystems | When integrating with legacy systems or external services | For systems with compatible models or simple interactions |
| 42 | Front Controller | Handles all requests for a website | When centralizing request handling logic in web applications | For simple websites with minimal request handling needs |
| 43 | Intercepting Filter | Processes requests and responses before and after main execution | When applying cross-cutting concerns to requests | When behavior is specific to request handlers and not common across requests |
| 44 | Business Delegate | Abstracts business services access details from presentation | When decoupling presentation from complex service interaction | For simple service access that doesn't require abstraction |
| 45 | Fluent Interface | Creates readable, method-chaining API | When API clarity and expressiveness are important | When method chaining complicates debugging or maintenance |
| 46 | Null Object | Provides neutral behavior for null references | When null checks clutter code and default behavior is needed | When explicit null handling is important for clarity |


444

Below is a comprehensive table that lists many of the design patterns commonly used in software development. The table includes the serial number, name, category (or type), a one-line description, as well as guidelines on when to use each pattern and when not to use it.

| Serial Number | Name of Design Pattern    | Category (Type)      | One Line Description                                                                                      | When to Use This Design Pattern                                                                                                         | When Not to Use This Design Pattern                                                                                       |
|---------------|---------------------------|----------------------|-----------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| 1             | Singleton                 | Creational           | Ensures a class has only one instance and provides a global point of access to it.                        | Use when exactly one instance of a class is needed to coordinate actions across the system.                                             | Avoid if multiple instances are needed or when global state can introduce hidden dependencies.                              |
| 2             | Factory Method            | Creational           | Defines an interface for creating an object, but lets subclasses decide which class to instantiate.       | Use when a class cannot anticipate the class of objects it must create and needs to delegate instantiation to subclasses.                  | Avoid when direct instantiation is straightforward and adds no real benefit in abstraction or extensibility.                |
| 3             | Abstract Factory          | Creational           | Provides an interface for creating families of related or dependent objects without specifying concrete classes. | Use when the system needs to be independent of how its objects are created, especially for creating related objects.                       | Avoid if there is only one type of product to create; the pattern may add unnecessary complexity.                          |
| 4             | Builder                   | Creational           | Separates the construction of a complex object from its representation, allowing different representations.   | Use when creating complex objects step by step is required and the same construction process can create different representations.     | Avoid if the object is simple and can be created using a constructor, as the extra abstraction may be overkill.              |
| 5             | Prototype                 | Creational           | Creates new objects by cloning an existing object, known as the prototype.                                | Use when the creation of an object is costly or complex, and a clone of an existing instance can be modified as needed.                   | Avoid if object copying is nontrivial or when deep cloning presents complications, leading to unintended shared state.      |
| 6             | Adapter                   | Structural           | Converts the interface of a class into another interface the clients expect.                             | Use when integrating classes with incompatible interfaces, especially with legacy or third-party components.                             | Avoid if the classes already have compatible interfaces or if a simple refactoring can directly align them.                   |
| 7             | Bridge                    | Structural           | Decouples an abstraction from its implementation so that the two can vary independently.                   | Use when both the abstractions and their implementations need to be extended independently, promoting flexibility.                       | Avoid if there is little likelihood of changes in both abstraction and implementation, as it can introduce extra complexity.   |
| 8             | Composite                 | Structural           | Composes objects into tree structures to represent part-whole hierarchies, allowing clients to treat individual objects and compositions uniformly. | Use when clients need to ignore the difference between individual objects and compositions, for example, in a graphical user interface.  | Avoid if object hierarchies are simple or if the added uniformity is unnecessary, as it may complicate the design.              |
| 9             | Decorator                 | Structural           | Dynamically attaches additional responsibilities to objects without modifying their structure.            | Use when you need to add responsibilities or behaviors to objects at runtime without using a proliferation of subclasses.                 | Avoid if the functionality can be achieved with simple inheritance, or when the dynamic addition of behavior is not required.  |
| 10            | Facade                    | Structural           | Provides a simplified, unified interface to a complex subsystem.                                         | Use when you want to hide the complexities of a subsystem and provide a simple interface to the client.                                    | Avoid if exposing the full capabilities of a subsystem is necessary or when the subsystem is already simple.                |
| 11            | Flyweight                 | Structural           | Reduces the cost of creating and manipulating a large number of similar objects by sharing common data.    | Use when many objects require a large memory footprint, and most of their state can be shared among instances.                              | Avoid if objects cannot share state safely or if the additional logic to manage shared state becomes overly complex.         |
| 12            | Proxy                     | Structural           | Provides a surrogate or placeholder for another object to control access to it.                           | Use when you need to add a layer of control, such as lazy loading, access control, or logging, to an underlying object.                     | Avoid if the extra level of indirection is not necessary, as it may hinder performance or complicate maintenance.            |
| 13            | Chain of Responsibility   | Behavioral           | Passes a request along a chain of handlers until one of them handles the request.                        | Use when multiple objects can handle a request and the handler is not known in advance, promoting loose coupling.                          | Avoid if there is a single obvious handler, or when the order of processing must be strictly defined and not dynamic.        |
| 14            | Command                   | Behavioral           | Encapsulates a request as an object, thereby letting you parameterize clients with queues, requests, or operations. | Use when you want to decouple the object that invokes the operation from the one that performs it, especially for undo/redo operations.  | Avoid if the interaction is simple enough that such encapsulation adds unnecessary overhead.                              |
| 15            | Interpreter               | Behavioral           | Defines a representation for a language's grammar along with an interpreter that uses the representation to interpret sentences in the language. | Use when a simple language needs to be interpreted or when you have a grammar that can be easily expressed in an object structure.          | Avoid if the language is complex or performance is critical, as the Interpreter pattern can be inefficient.                |
| 16            | Iterator                  | Behavioral           | Provides a way to access the elements of an aggregate object sequentially without exposing its underlying representation. | Use when you need to traverse a collection without exposing its internal structure, ensuring a consistent iteration interface.              | Avoid if the structure is simple or if other traversal mechanisms (like for-each loops in modern languages) provide sufficient functionality. |
| 17            | Mediator                  | Behavioral           | Defines an object that encapsulates how a set of objects interact, promoting loose coupling between them.   | Use when a group of objects communicate in complex ways, and you want to centralize the interaction logic in one mediator object.          | Avoid if the components are simple enough to interact directly, as introducing a mediator may add unnecessary complexity.    |
| 18            | Memento                   | Behavioral           | Captures and externalizes an object’s internal state so that it can be restored later without violating encapsulation. | Use when you need to implement undo/redo functionality or want to restore an object to a previous state easily.                            | Avoid if saving and restoring state is not a concern or if the object's state is not complex enough to warrant this approach.  |
| 19            | Observer                  | Behavioral           | Establishes a one-to-many dependency so that when one object changes state, all of its dependents are notified automatically. | Use when an object’s change in state needs to trigger updates in multiple other objects in a decoupled way.                                  | Avoid if the observer notifications could lead to performance issues or if updates need to be strictly controlled in a synchronous manner. |
| 20            | State                     | Behavioral           | Allows an object to change its behavior when its internal state changes, making it appear as if it changed its class. | Use when an object needs to alter behavior based on its state, and you want to encapsulate state-specific logic into separate classes.      | Avoid if the behavior variations are minimal or if adding separate state classes introduces unnecessary complexity.         |
| 21            | Strategy                  | Behavioral           | Encapsulates interchangeable algorithms and makes them interchangeable within that family.                 | Use when you need to select an algorithm at runtime from a family of algorithms, keeping the code flexible and decoupled from implementations. | Avoid if a single algorithm suffices or if the extra abstraction does not provide a clear benefit over simpler conditional logic. |
| 22            | Template Method           | Behavioral           | Defines the skeleton of an algorithm in a method, deferring some steps to subclasses without changing the algorithm's structure. | Use when a framework has a fixed workflow with steps that can vary among subclasses, promoting code reuse.                                   | Avoid if every step of the algorithm needs to be independent or if the rigidity of the template hinders necessary customization. |
| 23            | Visitor                   | Behavioral           | Separates an algorithm from the object structure it operates on, allowing you to add new operations without modifying the objects. | Use when you need to perform new operations on a set of objects without changing their classes, especially when dealing with a composite structure. | Avoid if the object structure is likely to change frequently or if the Visitor adds more complexity than necessary.           |
| 24            | Pipeline Pattern          | Architectural        | Arranges processing into a sequence of stages where each stage passes the output as input to the next stage. | Use when data processing benefits from being broken into isolated, reusable stages that can be assembled into a pipeline.                    | Avoid if the process is non-linear or when stages are too interdependent to be isolated cleanly.                           |
| 25            | Circuit Breaker           | Resiliency/Behavioral  | Prevents a system from repeatedly attempting an operation that is likely to fail by temporarily blocking requests. | Use when interacting with external services that may fail frequently so that failure is contained and recovery is handled gracefully.      | Avoid if external failures are rare or if built-in retry logic suffices, as it may add unnecessary overhead in stable systems. |
| 26            | Configuration Pattern     | Architectural/Structural | Centralizes configuration settings into a single component for easier management and environment-specific adjustments. | Use when an application’s behavior and settings need to be easily configurable and maintained without spreading configuration logic.    | Avoid if configuration is minimal or stable, as an elaborate configuration system can overcomplicate a simple application.    |

This table covers a broad set of design patterns—from traditional “Gang of Four” patterns to additional architectural and resiliency patterns—that are often used in software development along with practical guidance on their application.


555


Here is the complete set of software design patterns, categorized by type, with relevant details in table format:

| #  | Name of Design Pattern | Category/Type      | Description                                                              | When to Use                                                                 | When Not to Use                                                          |
|----|-------------------------|--------------------|--------------------------------------------------------------------------|-----------------------------------------------------------------------------|---------------------------------------------------------------------------|
| 1  | Singleton               | Creational         | Ensures only one instance of a class exists                              | When global access to a single instance is required                        | When multiple instances might be needed later                            |
| 2  | Factory Method          | Creational         | Delegates object creation to a factory method                            | When object creation logic needs encapsulation                             | For simple object creation with no complex logic                         |
| 3  | Abstract Factory        | Creational         | Creates families of related objects without specifying concrete classes  | When dealing with multiple product families                                | For single product type scenarios                                        |
| 4  | Builder                 | Creational         | Constructs complex objects step-by-step                                  | When creating objects with many optional parameters                        | For simple objects with fixed structure                                  |
| 5  | Prototype               | Creational         | Creates new objects by cloning existing instances                        | When object creation is costly compared to cloning                         | When deep copying isn't required                                         |
| 6  | Object Pool             | Creational         | Recycles objects to avoid expensive acquisition and release              | When managing limited resources like threads or database connections       | When resource acquisition is inexpensive                                 |
| 7  | Adapter                 | Structural         | Bridges incompatible interfaces between classes                          | When integrating legacy systems with new interfaces                        | When interfaces already match                                            |
| 8  | Bridge                  | Structural         | Decouples abstraction from implementation                                | When platform-independent abstractions are needed                          | For tightly coupled systems                                              |
| 9  | Composite               | Structural         | Treats individual objects and compositions uniformly                     | For part-whole hierarchies (e.g., GUI components)                           | When dealing with non-hierarchical structures                            |
| 10 | Decorator               | Structural         | Dynamically adds responsibilities to objects                             | For flexible extension of functionality                                    | When using static inheritance suffices                                   |
| 11 | Facade                  | Structural         | Provides simplified interface to complex subsystems                      | To reduce subsystem complexity for clients                                 | When direct subsystem access is necessary                                |
| 12 | Flyweight               | Structural         | Shares common state between objects to reduce memory usage               | When dealing with many similar objects (e.g., text formatting)             | For objects with unique states                                           |
| 13 | Proxy                   | Structural         | Controls access to another object                                        | For lazy initialization, access control, or logging                        | When direct access is sufficient                                         |
| 14 | Chain of Responsibility| Behavioral         | Passes requests through a chain of handlers                              | When multiple objects might handle a request                               | When request has single handler                                          |
| 15 | Command                 | Behavioral         | Encapsulates requests as objects                                         | For queuing requests, undo/redo functionality                              | For simple direct execution                                              |
| 16 | Interpreter             | Behavioral         | Implements language grammar using class hierarchy                        | For domain-specific language implementation                                | For general-purpose language parsing                                     |
| 17 | Iterator                | Behavioral         | Provides sequential access to collection elements                        | When abstracting collection traversal logic                                | For simple collections with direct access                                |
| 18 | Mediator                | Behavioral         | Centralizes complex communication between objects                        | When object interactions become too complex                                | For simple direct communication scenarios                                |
| 19 | Memento                 | Behavioral         | Captures and restores object state                                       | For undo/redo functionality or snapshots                                   | When object state is simple or immutable                                 |
| 20 | Null Object             | Behavioral         | Provides default behavior for absent objects                             | To avoid null checks in client code                                        *When null references are acceptable                                      |
| 21* Observer                *Behavioral          *Notifies dependent objects about state changes                           *For event-driven systems or publish-subscribe scenarios                     *When tight coupling is acceptable                                        *

Citations:
[1] https://en.wikipedia.org/wiki/Category:Software_design_patterns
[2] https://www.scaler.com/topics/design-patterns/types-of-design-pattern/
[3] https://wezom.com/blog/design-patterns-in-software-development
[4] https://stfalcon.com/en/blog/post/10-Software-Design-Patterns-Every-Developer-Must-Know
[5] https://sourcemaking.com/design_patterns
[6] https://www.digitalocean.com/community/tutorials/gangs-of-four-gof-design-patterns
[7] https://www.netsolutions.com/insights/software-design-pattern/
[8] https://refactoring.guru/design-patterns/catalog
[9] https://www.youtube.com/watch?v=tv-_1er1mWI
[10] https://refactoring.guru/design-patterns/classification
[11] https://refactoring.guru/design-patterns/catalog
[12] https://www.youtube.com/watch?v=BJatgOiiht4
[13] https://sourcemaking.com/design_patterns
[14] https://refactoring.guru/design-patterns
[15] https://en.wikipedia.org/wiki/Software_design_pattern
[16] https://www.blazemeter.com/blog/software-design-patterns
[17] https://swimm.io/learn/system-design/the-top-7-software-design-patterns-you-should-know-about
[18] https://www.tpointtech.com/classification-of-design-pattern-in-python
[19] https://dev.to/leoat12/design-patterns-use-or-not-to-use-22h6
[20] https://developers.hubspot.com/docs/reference/ui-components/design/patterns/tables
[21] https://www.devleader.ca/2023/12/31/the-big-list-of-design-patterns-everything-you-need-to-know
[22] https://www.pencilandpaper.io/articles/ux-pattern-analysis-enterprise-data-tables
[23] https://ui-patterns.com/patterns
[24] https://www.smashingmagazine.com/2019/01/table-design-patterns-web/
[25] https://dzone.com/refcardz/design-patterns
[26] https://learn.microsoft.com/en-us/azure/storage/tables/table-storage-design-patterns
[27] https://fullscale.io/blog/design-patterns-3-types-and-their-applications/
[28] https://www.linkedin.com/pulse/classification-design-patterns-rainer-grimm
[29] https://www.freecodecamp.org/news/the-basic-design-patterns-all-developers-need-to-know/
[30] https://vertabelo.com/blog/database-design-patterns/
[31] https://www.indeed.com/career-advice/career-development/types-of-design-patterns
[32] https://wezom.com/blog/design-patterns-in-software-development

---
Answer from Perplexity: pplx.ai/share


