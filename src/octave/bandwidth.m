
A=mmread("../../datasets/webBerkStan.mtx")

# reverse
#minimum = amd(A)

#figure();
#subplot(1,2,1),spy(A),title('A')

# The reverse Cuthill-McKee ordering is obtained with:
p = symrcm(A);
R = A(p,p);

mmwrite("webBerkStan-reordered.mtx", R, "Reordered using reverse Cuthill-McKee ordering")

#subplot(1,2,2),spy(R),title('A(p,p)')