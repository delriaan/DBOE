# Copy this file into the package project root; edit as desired before executing
this.desc <- desc::desc();
this.desc$print();

people = {list(
		Chionesu = person(
			given = "Chionesu"
			, family = "George"
			, email = "cgeorge@alliancehealthplan.org"
			, role = c("aut", "cre")
		), Heather = person(
				given = "Heather"
				, family = "Copley"
				, email = "hcopley@alliancehealthplan.org"
				, role = c("aut", "cre")
			)
			, DScience = person(
				given = "Alliance Health Data Science Department"
				, email = "DScience@alliancehealthplan.org"
			)
		)};

deps <- { data.table::data.table(
		type = c(rep.int("Depends", 4), "Imports")
		, package = c("tictoc", "purrr", "DBI", "data.table", "book.of.utilities", "book.of.workflow")
		, version = "*"
		)}

this.desc$set("Author", people$C)
this.desc$set("Maintainer", people$D)
this.desc$set("License", "Unicorn Overlords")
this.desc$set("Desc", "Explore DBMS metadata.")
this.desc$set("Title", "Datbase Object Explorer")
this.desc$set_version("0.1.0")
this.desc$set_deps(deps)
this.desc$write()
this.desc$print()

