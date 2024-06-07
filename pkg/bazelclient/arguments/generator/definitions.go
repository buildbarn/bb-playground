// Copyright The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

var enumTypes = map[string][]string{
	"HelpVerbosity": {
		"long",
		"medium",
		"short",
	},
}

var flagSets = map[string]flagSet{
	"build": {
		flags: []flag{
			{
				longName:    "keep_going",
				shortName:   "k",
				description: "Continue as much as possible after an error. While the target that failed and those that depend on it cannot be analyzed, other prerequisites of these targets can be.",
				flagType: boolFlagType{
					defaultValue: false,
				},
			},
		},
	},
	"help": {
		flags: []flag{
			{
				longName:    "help_verbosity",
				description: "Select the verbosity of the help command.",
				flagType: enumFlagType{
					enumType:     "HelpVerbosity",
					defaultValue: "medium",
				},
			},
			{
				longName:    "long",
				shortName:   "l",
				description: "Show full description of each option, instead of just its name.",
				flagType: expansionFlagType{
					expandsTo: []string{
						"--help_verbosity=long",
					},
				},
			},
			{
				longName:    "short",
				description: "Show only the names of the options, not their types or meanings.",
				flagType: expansionFlagType{
					expandsTo: []string{
						"--help_verbosity=short",
					},
				},
			},
		},
	},
	"startup": {
		flags: []flag{
			{
				longName:    "bazelrc",
				description: "The location of the user .bazelrc file containing default values of Bazel options. /dev/null indicates that all further `--bazelrc`s will be ignored, which is useful to disable the search for a user rc file, e.g. in release builds. This option can also be specified multiple times. E.g. with `--bazelrc=x.rc --bazelrc=y.rc --bazelrc=/dev/null --bazelrc=z.rc`, 1) x.rc and y.rc are read. 2) z.rc is ignored due to the prior /dev/null. If unspecified, Bazel uses the first .bazelrc file it finds in the following two locations: the workspace directory, then the user's home directory. Note: command line options will always supersede any option in bazelrc.",
				flagType:    stringListFlagType{},
			},
			{
				longName:    "home_rc",
				description: "Whether or not to look for the home bazelrc file at $HOME/.bazelrc.",
				flagType: boolFlagType{
					defaultValue: true,
				},
			},
			{
				longName:    "ignore_all_rc_files",
				description: "Disables all rc files, regardless of the values of other rc-modifying flags, even if these flags come later in the list of startup options.",
				flagType: boolFlagType{
					defaultValue: false,
				},
			},
			{
				longName:    "system_rc",
				description: "Whether or not to look for the system-wide bazelrc.",
				flagType: boolFlagType{
					defaultValue: true,
				},
			},
			{
				longName:    "workspace_rc",
				description: "Whether or not to look for the workspace bazelrc file at $workspace/.bazelrc.",
				flagType: boolFlagType{
					defaultValue: true,
				},
			},
		},
	},
}

var commands = map[string]command{
	"build": {
		flagSets:       []string{"build"},
		takesArguments: true,
	},
	"clean": {
		flagSets: []string{},
	},
	"help": {
		flagSets:       []string{"help"},
		takesArguments: true,
	},
}
