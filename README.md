PostgreSQL Database Management System
=====================================

This directory contains the source code distribution of the PostgreSQL
database management system.

PostgreSQL is an advanced object-relational database management system
that supports an extended subset of the SQL standard, including
transactions, foreign keys, subqueries, triggers, user-defined types
and functions. This distribution also contains C language bindings.

PostgreSQL has many language interfaces, many of which are listed here:
https://www.postgresql.org/download

See the file INSTALL for instructions on how to build and install
PostgreSQL. That file also lists supported operating systems and
hardware platforms and contains information regarding any other
software packages that are required to build or run the PostgreSQL
system. Copyright and license information can be found in the
file COPYRIGHT. A comprehensive documentation set is included in this
distribution; it can be read as described in the installation
instructions.

The latest version of this software may be obtained at
https://www.postgresql.org/download/. For more information look at our
web site located at https://www.postgresql.org/.

---

# EHJ Modification Notes

Original Hash Join built into postgres was Hybrid Hash Join. Hash join implementation modified in place to Early Hash Join as described in the [Early Hash Join paper](https://cmps-people.ok.ubc.ca/rlawrenc/research/Papers/EarlyHashJoin.pdf).

## Plan Tree 

### Plan Tree Inheritance 

**Node** → **Plan** → **Join** → **HashJoin**

### Planned Changes to Plan Tree Structs

**Name:** **Definition in:** **Significance:** **Overview of Changes:** 

**Name:** **Definition in:** **Significance:** **Overview of Changes:**

## Execution State Tree

### Execution State Tree Inheritance 

**Node** → **PlanState** → **JoinState** → **HashJoinState**

### Planned Changes to Execution State Tree Structs

**Name:** `HashJoinState`  
**Definition in:** `src/include/nodes/execnodes.h`  
**Significance:** **Overview of Changes:** 
