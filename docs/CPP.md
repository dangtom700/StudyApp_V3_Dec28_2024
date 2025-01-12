This library contains all C++ headers that are needed in this program.
The role of C++ program is to accelerate the number cruching process that Python
is pretty slow on.

This is included:
- env.hpp: storing all environment variables that are needed for the C++ program
- utilities.hpp: storing all utility functions that are needed for the C++ program
- transform.hpp: storing all transformation functions that are needed for the C++ program
- feature.hpp: storing all feature functions that are needed for the C++ program
- updateDB.hpp: storing all update functions dedicated for database information update that are needed for the C++ program

Library dependency:
|_env.hpp
|       |_utilities.hpp
|       |_transform.hpp
|       |_feature.hpp
|       |_updateDB.hpp
|
|_transform.hpp
|       |_feature.hpp
|
|_updateDB.hpp
|       |_feature.hpp
|
|_feature.hpp
|
|_utilities.hpp
|       |_transform.hpp
|       |_feature.hpp
|       |_updateDB.hpp