""" OutputWriter.py """
from MyFunctions import *


class OutputWriter(object):
    """ Write output to standard out or file.

    Attributes:
        out_file_name (str): relative or absolute path to output file,
            or '' for standard output.
        out_file (object): handle to opened file with name = out_file_name.
        align_col (bool): pad values with spaces to align columns?
        col_sep (str): character(s) to separate columns in output.
            Common choices:
            "" (no characters)
            " " (single space, aka chr(32))
            chr(9) (aka the horizontal tab character)
            "|"
            ","
    """
    def __init__(self, out_file_name: str = '', align_col: bool = True,
                 col_sep: str = ',') -> None:
        """ Constructor method for this class.

        Parameters:
            out_file_name (str): relative or absolute path to output file, or ''
                for standard output.
            align_col (bool): if True, pad columns with spaces in the output so
                they always have the same width.
            col_sep (str): character(s) to separate columns in output.
                Common choices:
                "" (no characters)
                " " (single space, aka chr(32))
                chr(9) (aka the horizontal tab character)
                "|"
                ","
        Returns:
        """
        out_file = None
        if out_file_name == '':
            out_file = sys.stdout
        else:
            try:
                out_file = open(out_file_name, 'w')
            except OSError:
                print_stacktrace()
                exit(1)

        self.out_file_name: str = out_file_name
        self.out_file = out_file
        self.align_col: bool = align_col
        self.col_sep: str = col_sep
        return
    # End of method __init__.

    def close_output_file(self):
        """ Close output file, if it exists.

        Parameters:
        Returns:
        """
        if self.out_file_name != '':
            self.out_file.close()
        return
    # End of method close_output_file.

    def get_align_col(self):
        """ Prompt for align_col: chaice to align or not align columns.

        Parameters:
        Returns:
        """
        prompt = '\nAlign columns (pad with blanks)?  Enter "Y" or "N":\n'
        # Keep looping until have acceptable answer.
        while True:
            response = input(prompt).strip().upper()
            if response == '':
                print('Invalid answer, please try again.')
            elif response in 'YT1':
                print('You chose to align columns.')
                self.align_col = True
                break
            elif response in 'NF0':
                print('You chose to not align columns.')
                self.align_col = False
                break
            else:
                print('Invalid answer, please try again.')
        return
    # End of method get_align_col.

    def get_col_sep(self):
        """ Prompt for col_sep: character(s) to separate columns.

        Parameters:
        Returns:
        """
        prompt = '\nEnter column separator character(s):\n'
        self.col_sep = input(prompt)
        print('You chose separate columns with "{}".'.format(self.col_sep))
        return
    # End of method get_col_sep.

    def get_out_file_name(self):
        """ Prompt for relative or absolute path to output file.

        Parameters:
        Returns:
        """
        from os.path import dirname, isdir
        prompt = ('\nEnter the name and relative or absolute'
                  '\nlocation of the file to write output to.'
                  '\nOr hit Return to print to the standard output:\n')
        # Keep looping until able to open output file, or "" entered.
        while True:
            out_file_name = input(prompt).strip()
            if out_file_name == '':
                out_file = sys.stdout
                break
            else:
                dir_name = dirname(out_file_name)
                if isdir(dir_name):
                    try:
                        out_file = open(out_file_name, 'w')
                        break
                    except OSError:
                        print_stacktrace()
                else:
                    print('Invalid directory specified: ' + dir_name)

        self.out_file = out_file
        self.out_file_name = out_file_name
        if self.out_file_name == '':
            print('You chose to write to the standard output.')
        else:
            print('Your output file is "{}".'.format(self.out_file_name))
        return
    # End of method get_out_file_name.

    def write_rows(self, all_rows: list, col_names: list) -> None:
        """ Write rows in the output of SQL to chosen destination.

        Parameters:
            all_rows (list): list of tuples, each tuple a row.
            col_names (list): list of column names.
                None means do not write column headers and line of dashes below.
        Returns:
        """
        # Put quotes around columns containing col_sep.
        if self.col_sep != '':
            # Loop through rows.
            for item_num1, row in enumerate(all_rows):
                changed = False
                # Loop through columns in each row.
                for item_num2, column in enumerate(row):
                    # Update row when needed.
                    if self.col_sep in str(column):
                        # Convert tuple to list to make row mutable.
                        if not changed:
                            row = list(row)
                            changed = True
                        # Enclose values containing col_sep in quotes,
                        # double quotes to escape them.
                        row[item_num2] = "'" + str(column).replace("'", "''") + "'"
                # Save updated version of row if changed = True.
                if changed:
                    all_rows[item_num1] = tuple(row)

        # Column name widths.
        if col_names is not None:
            col_sizes = [len(col_name) for col_name in col_names]
        else:
            # Not printing column names, use data row 0 to start col_sizes calc.
            col_sizes = [len(row) for row in all_rows[0]]

        if self.align_col:
            # Column widths to align columns, make just wide enough.
            for row in all_rows:
                col_sizes = [max(size, len(str(col))) for (size, col) in
                             zip(col_sizes, row)]

        if col_names is not None:
            # Format and print the column names.
            formats = ['{{:^{}}}'.format(size) for size in col_sizes]
            col_names_fmt = self.col_sep.join(formats)
            self.out_file.write(col_names_fmt.format(*col_names))

            # Print line of dashes below the column names.
            dashes = ['-' * col_size for col_size in col_sizes]
            self.out_file.write('\n' + self.col_sep.join(dashes))

        # Find format string for the rows.
        if self.align_col:
            formats = ['{{:{}}}'.format(size) for size in col_sizes]
        else:
            formats = ['{}']*len(col_sizes)
        row_fmt = self.col_sep.join(formats)

        # Print the rows.
        for row in all_rows:
            row = ['' if x is None else x for x in row]
            self.out_file.write('\n' + row_fmt.format(*row))
        # If printed to file, announce that.
        if self.out_file_name != '':
            print('Just wrote output to "{}".'.format(self.out_file_name))
        return
    # End of method write_rows.
# End of Class OutputWriter.
