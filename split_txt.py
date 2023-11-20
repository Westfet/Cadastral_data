import os


def split_txt_to_multiple_files(input_txt_file, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    count = 1
    with open(input_txt_file, 'r', encoding='utf-8') as txtfile:
        lines = txtfile.readlines()
        total_lines = len(lines)

        for i in range(0, total_lines, 300):
            output_file = os.path.join(output_folder, f"{count}.txt")
            with open(output_file, 'w', encoding='utf-8') as outfile:
                cadastre_numbers = ",".join(map(str.strip, lines[i:i + 300]))
                outfile.write(cadastre_numbers)
            count += 1


split_txt_to_multiple_files('list_example.txt', 'txt_folder')
