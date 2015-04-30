Stasis is a flexible transactional storage library that is geared toward high-performance applications and system developers. It supports concurrent transactional storage, and no-FORCE/STEAL buffer management.

Traditionally, write-ahead-logging schemes have been closely coupled to a relational database or other high-level system, making it difficult to make full use of their functionality without making significant changes to complex and tightly coupled code. Libraries such as Berkeley DB also provide lower level interfaces, but focus on providing a narrow set of highly configurable transactional structures. Stasis focuses on providing applications an easy to use toolkit for building their own transactional structures.

Stasis achieves this by providing clean interfaces between its subsystems. We hope that this will allow for the implementation of self-tuning primitives, reducing the amount of work required to build high-performance storage systems.  Although full redo/undo logging (with nested top actions) is the default, Stasis provides a number of logging modes, including unlogged updates to newly allocated storage.  It is appropriate for both log-structured and update-in-place storage structures.

# Current stable release #
We have not yet released a stable version of Stasis.  However, the 0.8 branch is currently in feature freeze, and is beta quality code.  It is available at:
```
svn co http://stasis.googlecode.com/svn/branches/stasis-0.8
```
Development (including significant performance enhancements) continues in SVN trunk:
```
svn co http://stasis.googlecode.com/svn/trunk
```

# Getting Started #
The API documentation contains an [introduction and tutorial](http://hedora.ath.cx/doc/).

For more detailed information, see the PhD dissertation: [Stasis: Flexible Transactional Storage](http://www.eecs.berkeley.edu/Pubs/TechRpts/2010/EECS-2010-2.html)
  * Part I is a survey of data models, with a focus on their storage implications.
  * Part II targets Stasis application developers, and people interested in implementing new indexes and other transactional primitives.
  * Part III describes Stasis' internals, and is geared toward developers that would like to make use of exotic recovery mechanisms or that would like to modify Stasis' internals.