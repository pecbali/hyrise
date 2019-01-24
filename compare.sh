compare () {
	if [ -f $2 ]; then
		if [ -f $3 ]; then
			echo $1
			echo "scripts/compare_benchmarks.py $2 $3"
			python scripts/compare_benchmarks.py $2 $3
		fi
	fi
}
if [ "$#" -lt 1 ]; then
    echo "Provide dir for results"
else
	dir=$1
	if [ ! -d $dir ]; then
  		echo "Directory $dir does not exist"
		exit
	fi
	if [ "${dir: -1}" != "/" ]; then
		dir=$dir"/"
	fi
	if [ "$#" -eq 1 ]; then
		cat ${dir}*.txt
		compare "no jit vs jit without mvcc" ${dir}result_wo_jit_wo_mvcc.json ${dir}result_w_jit_wo_mvcc.json
		compare "no jit vs jit with mvcc" ${dir}result_wo_jit_w_mvcc.json ${dir}result_w_ll_w_jv.json
		compare "no jit vs jit with mvcc but not jit validate" ${dir}result_wo_jit_w_mvcc.json ${dir}result_w_ll_wo_jv.json
		compare "lazy load" ${dir}result_wo_ll_wo_jv.json ${dir}result_w_ll_wo_jv.json
		compare "jit validate" ${dir}result_wo_ll_wo_jv.json ${dir}result_wo_ll_w_jv.json
		compare "lazy load with jit validate enabled" ${dir}result_wo_ll_w_jv.json ${dir}result_w_ll_w_jv.json
		compare "jit validate with lazy load enabled" ${dir}result_w_ll_wo_jv.json ${dir}result_w_ll_w_jv.json
		compare "lazy load and jit validate" ${dir}result_wo_ll_wo_jv.json ${dir}result_w_ll_w_jv.json
	else
		r_dir=$2
		if [ ! -d $r_dir ]; then
	  		echo "Right directory $dir does not exist"
			exit
		fi
		if [ "${r_dir: -1}" != "/" ]; then
			r_dir=$r_dir"/"
		fi
		echo "prev:"
		cat ${dir}*.txt
		echo "next:"
		cat ${r_dir}*.txt
		declare -a files=("result_wo_jit_w_mvcc.json" "result_wo_jit_wo_mvcc.json" "result_wo_ll_wo_jv.json" "result_w_ll_wo_jv.json" "result_wo_ll_w_jv.json" "result_w_ll_w_jv.json" "result_w_jit_wo_mvcc.json")
		for i in "${files[@]}"
		do
			echo $i
			python scripts/compare_benchmarks.py ${dir}${i} ${r_dir}${i}
		done
	fi
fi


